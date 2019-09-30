/*
 * Copyright 2015 Webtrends (http://www.webtrends.com)
 *
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.webtrends.harness.component.kafka.actor

import akka.actor.{Actor, ActorRef, Props}
import com.webtrends.harness.component.kafka.health.KafkaWriterHealthCheck
import com.webtrends.harness.component.kafka.util.{KafkaSettings, KafkaUtil}
import com.webtrends.harness.component.metrics.metrictype.Meter
import com.webtrends.harness.health.{ComponentState, HealthComponent}
import com.webtrends.harness.logging.ActorLoggingAdapter
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

object KafkaWriter {
  case class KafkaMessage(topic: String,
                          data: Array[Byte],
                          key: Option[Array[Byte]] = None,
                          partition: Option[Any] = None)

  case class MessageToWrite(message: KafkaMessage)

  case class MessagesToWrite(messages: List[KafkaMessage])

  case class MessagesToWriteWithAck(ackId: String, messages: KafkaMessage*)(implicit val ec: ExecutionContext)
  private object MessagesToWriteWithAck {
    def unapply(message: MessagesToWriteWithAck) = Some((message.ackId, message.messages, message.ec))
  }

  case class MessageWriteAck(ackId: Either[String, SendFailureException])

  // ackId will be the ackId set in MessageToWriteWithAck
  case class SendFailureException(ackId: String, error: Throwable)
    extends Throwable(s"Ack'd id [$ackId] failed to produce", error)

  def props(parent: ActorRef): Props = Props(classOf[KafkaWriter], parent)
}

class KafkaWriter(val healthParent: ActorRef) extends Actor
  with ActorLoggingAdapter with KafkaWriterHealthCheck with KafkaSettings {
  import KafkaWriter._

  lazy val totalEvents = Meter("total-events-per-second")
  lazy val totalBytesPerSecond = Meter("total-bytes-per-second")

  val dataProducer = newProducer
  val topicPartsMeta = mutable.HashMap[String, List[PartitionInfo]]()

  def newProducer = new KafkaProducer[Array[Byte], Array[Byte]](KafkaUtil.configToProps(kafkaConfig.getConfig("producer")))

  override def postStop() = {
    log.info("Stopping Kafka Writer")
    super.postStop()
    dataProducer.close()
  }

  def receive:Receive = {
    case MessageToWrite(message) => sendData(Seq(message))

    case MessagesToWrite(messages) => sendData(messages)

    case MessagesToWriteWithAck(ackId, messages, ec) =>
      sendDataAck(messages, ackId, sender())(ec)
  }

  def sendData(eventMessages: Seq[KafkaMessage], ackId: String = ""): Unit =
    Try {
      sendMessages(eventMessages)
    } recover {
      case ex: Exception =>
        log.error(s"Unable To write Event, ackId=[$ackId]", ex)
        setHealth(HealthComponent(self.path.name, errorState, "Last message failed to send"))
    }

  def sendDataAck(eventMessages: Seq[KafkaMessage], ackId: String, sender: ActorRef)(implicit ec: ExecutionContext): Unit =
    sendMessagesAck(eventMessages) onComplete {
      case _ if currentHealth.state != ComponentState.NORMAL =>
        sender ! MessageWriteAck(Right(SendFailureException(ackId, new IllegalStateException("Producer is not healthy"))))
      case Success(_) =>
        sender ! MessageWriteAck(Left(ackId))
      case Failure(ex: Exception) =>
        log.error(s"Unable To write Event, ackId=[$ackId]", ex)
        setHealth(HealthComponent(self.path.name, errorState, "Last message failed to send"))
        sender ! MessageWriteAck(Right(SendFailureException(ackId, ex)))
    }

  protected def keyedMessage(kafkaMessage: KafkaMessage): ProducerRecord[Array[Byte], Array[Byte]] = {
    val partition = kafkaMessage.partition.orNull match {
      case null => null
      case i: Int => Integer.valueOf(i)
      case arr: Array[Byte] => Integer.valueOf(Utils.abs(java.util.Arrays.hashCode(arr)) % getPartNum(kafkaMessage.topic))
      case a => Integer.valueOf(Utils.abs(a.hashCode) % getPartNum(kafkaMessage.topic))
    }
    val key = kafkaMessage.key.orNull
    new ProducerRecord[Array[Byte], Array[Byte]](kafkaMessage.topic, partition, key, kafkaMessage.data)
  }

  protected def sendMessages(messages: Seq[KafkaMessage]): Unit = {
    //iterator vs. foreach for performance gains
    val itr = messages.iterator
    while (itr.hasNext) {
      val kafkaMessage = itr.next()
      val message = keyedMessage(kafkaMessage)

      // If the Kafka target is down completely, the Kafka client provides no feedback.
      // The callback is never called and a get on the returned java Future will block indefinitely.
      // Handling this in monitorSendHealth
      val productionFuture = dataProducer.send(message)
      monitorSendHealth(productionFuture)

      updateMetrics(kafkaMessage)
    }
  }

  // This one relies on all messages to successfully write to kafka. Unlike sendMessages method which is
  // entirely fire and forget, this one does not resolve until all messages have successfully written. Do not
  // use this method if super fast performance is your only concern. Use for reliability and logging.
  protected def sendMessagesAck(messages: Seq[KafkaMessage]): Future[Unit] =
    Future.sequence(messages.map(m => {
      val p = Promise[Unit]()
      val message = keyedMessage(m)
      dataProducer.send(message, new JavaCallback(p))
      updateMetrics(m)
      p.future
    })

  private def updateMetrics(message: KafkaMessage): Unit = {
    totalEvents.mark(1)

    if (message != null && message.data != null)
      totalBytesPerSecond.mark(message.data.length)
  }

  private def getPartNum(topic: String): Int = {
    topicPartsMeta.get(topic) match {
      case Some(parts) => parts.length
      case None =>
        val parts = dataProducer.partitionsFor(topic)
        topicPartsMeta.put(topic, parts.toList)
        parts.length
    }
  }
}

private class JavaCallback(p: Promise[Unit]) extends Callback {
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
    (metadata, exception) match {
      case (null, e: Exception) =>
        p.failure(e)
      case (_: RecordMetadata, null) =>
        p.success()
    }
}