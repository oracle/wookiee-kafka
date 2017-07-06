package com.webtrends.harness.component.kafka.actor
import akka.actor._
import akka.testkit.TestProbe
import com.webtrends.harness.component.kafka.actor.KafkaWriter.KafkaMessage
import com.webtrends.harness.component.kafka.config.KafkaTestConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.runner.RunWith
import org.specs2.mutable.SpecificationLike
import org.specs2.runner.JUnitRunner

import scala.concurrent.duration.{Duration, SECONDS}

case class TestRequest(kafkaMessage: KafkaMessage)
case class TestResponse(producerRecord: ProducerRecord[Array[Byte], Array[Byte]])

class KafkaWriterTester extends KafkaWriter {
  override def receive:Receive = {
    case TestRequest(message) =>
      sender ! TestResponse(keyedMessage(message))
  }

  override def postStop() = {

  }

  override  def newProducer = null
}

object KafkaWriterTester {
  def props(): Props = Props[KafkaWriterTester]
}

@RunWith(classOf[JUnitRunner])
class KafkaWriterSpec extends  SpecificationLike {
  implicit val system = ActorSystem("test", KafkaTestConfig.config)
  val kafkaWriter = system.actorOf(KafkaWriterTester.props(), "kafka-writer-tester")
  val probe = TestProbe()
  val duration = Duration(5, SECONDS)
  val msgData = "msg".getBytes("UTF-8")

  sequential

  "KafkaWriter" should {
    "honor msg key" in {
      val msgKey = "messageKey"
      val kafkaMessage = KafkaMessage("topic", msgData, Some(msgKey.getBytes))

      probe.send(kafkaWriter, TestRequest(kafkaMessage))
      val response = probe.receiveOne(duration)
      val result = response match {
        case TestResponse(record) =>
          new String(record.key).equals(msgKey)
        case _ =>
          false
      }

      result mustEqual true
    }

    "honor msg key as bytes" in {
      val msgKey = "messageKey"
      val kafkaMessage = KafkaMessage("topic", msgData, Some(msgKey.getBytes))

      probe.send(kafkaWriter, TestRequest(kafkaMessage))
      val response = probe.receiveOne(duration)
      val result = response match {
        case TestResponse(record) =>
          new String(record.key).equals(msgKey)
        case _ =>
          false
      }

      result mustEqual true
    }

    "allow missing msg key" in {
      val kafkaMessage = KafkaMessage("topic", msgData)

      probe.send(kafkaWriter, TestRequest(kafkaMessage))
      val response = probe.receiveOne(duration)
      val result = response match {
        case TestResponse(record) =>
          record.key == null
        case _ =>
          false
      }

      result mustEqual true
    }

    "honor partition key" in {
      val partition = 1
      val kafkaMessage = KafkaMessage("topic", msgData, None, Some(partition))

      probe.send(kafkaWriter, TestRequest(kafkaMessage))
      val response = probe.receiveOne(duration)
      val result = response match {
        case TestResponse(record) =>
          record.partition().equals(partition)
        case _ =>
          false
      }

      result mustEqual true
    }

    "allow missing partition" in {
      val kafkaMessage = KafkaMessage("topic", msgData)

      probe.send(kafkaWriter, TestRequest(kafkaMessage))

      val partition: Integer = probe.receiveOne(duration) match {
        case TestResponse(record) =>
          record.partition()
        case _ => -1
      }

      partition mustEqual null

    }
  }
}



