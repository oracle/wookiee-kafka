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
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Try

object KafkaWriter {
  case class KafkaMessage(topic: String,
                          data: Array[Byte],
                          key: Option[Array[Byte]] = None,
                          partition: Option[Any] = None)

  case class MessageToWrite(message: KafkaMessage)

  case class MessagesToWrite(messages: List[KafkaMessage])

  case class MessagesToWriteWithAck(ackId: String, messages: KafkaMessage*)

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

    case msgsWithAck: MessagesToWriteWithAck =>
      sender() ! MessageWriteAck(sendData(msgsWithAck.messages, msgsWithAck.ackId))
  }

  def sendData(eventMessages: Seq[KafkaMessage], ackId: String = ""): Either[String, SendFailureException] = {
    Try {
      sendMessages(eventMessages)

      if (currentHealth.state == ComponentState.NORMAL) {
        Left(ackId)
      } else {
        Right(SendFailureException(ackId, new IllegalStateException("Producer is not healthy")))
      }
    } recover {
      case ex: Exception =>
        log.error(s"Unable To write Event, ackId=[$ackId]", ex)
        setHealth(HealthComponent(self.path.name, errorState, "Last message failed to send"))
        Right(SendFailureException(ackId, ex))
    } get
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
      if (kafkaMessage.data.length > 0) {
        val message = keyedMessage(kafkaMessage)

        // If the Kafka target is down completely, the Kafka client provides no feedback.
        // The callback is never called and a get on the returned java Future will block indefinitely.
        // Handling this by waiting on the
        val productionFuture = dataProducer.send(message)
        monitorSendHealth(productionFuture)

        totalBytesPerSecond.mark(kafkaMessage.data.length)
        totalEvents.mark(1)
      }
    }
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
