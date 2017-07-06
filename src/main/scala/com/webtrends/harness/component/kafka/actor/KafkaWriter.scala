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

import akka.actor.{Actor, Props}
import com.webtrends.harness.component.kafka.health.KafkaWriterHealthCheck
import com.webtrends.harness.component.kafka.util.{KafkaUtil, KafkaSettings}
import com.webtrends.harness.component.metrics.metrictype.Meter
import com.webtrends.harness.health.ComponentState
import com.webtrends.harness.logging.ActorLoggingAdapter
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.util.Try

object KafkaWriter {
  case class KafkaMessage(topic: String,
                          data: Array[Byte],
                          key: Option[Array[Byte]] = None,
                          partition: Option[Integer] = None)

  case class MessageToWrite(message: KafkaMessage)

  case class MessagesToWrite(messages: List[KafkaMessage])

  case class MessagesToWriteWithAck(ackId: String, messages: KafkaMessage*)

  case class MessageWriteAck(ackId: Either[String, Throwable])

  def props(): Props = Props[KafkaWriter]
}

class KafkaWriter extends Actor
  with ActorLoggingAdapter with KafkaWriterHealthCheck with KafkaSettings {
  import KafkaWriter._

  lazy val totalEvents = Meter("total-events-per-second")
  lazy val totalBytesPerSecond = Meter("total-bytes-per-second")

  val dataProducer = newProducer

  def newProducer = new KafkaProducer[Array[Byte], Array[Byte]](KafkaUtil.configToProps(kafkaConfig.getConfig("producer")))

  override def postStop() = {
    log.info("Stopping Kafka Writer")
    super.postStop()
    dataProducer.close()
  }

  def receive:Receive = healthReceive orElse {
    case MessageToWrite(message) => sendData(Seq(message))

    case MessagesToWrite(messages) => sendData(messages)

    case msgsWithAck: MessagesToWriteWithAck =>
      sender() ! MessageWriteAck(sendData(msgsWithAck.messages, msgsWithAck.ackId))
  }

  def sendData(eventMessages: Seq[KafkaMessage], ackId: String = ""): Either[String, Throwable] = {
    Try {
      //iterator vs. foreach for performance gains
      val itr = eventMessages.iterator
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

      if (currentHealth.isEmpty || currentHealth.get.state == ComponentState.NORMAL) {
        Left(ackId)
      }
      else {
        Right(new IllegalStateException("Producer is not healthy"))
      }
    } recover {
      case ex: Exception =>
        log.error("Unable To write Event", ex)
        setHealth(ComponentState.CRITICAL, "Last message failed to send")
        Right(ex)
    } get
  }

  protected def keyedMessage(kafkaMessage: KafkaMessage): ProducerRecord[Array[Byte], Array[Byte]] = {
    val partition = kafkaMessage.partition.orNull
    val key = kafkaMessage.key.orNull
    new ProducerRecord[Array[Byte], Array[Byte]](kafkaMessage.topic, partition, key, kafkaMessage.data)
  }
}
