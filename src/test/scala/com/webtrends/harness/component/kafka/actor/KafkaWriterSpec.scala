package com.webtrends.harness.component.kafka.actor
import akka.actor._
import akka.testkit.TestProbe
import com.webtrends.harness.component.kafka.actor.KafkaWriter.{KafkaMessage, MessageWriteAck, MessagesToWriteWithAck}
import com.webtrends.harness.component.kafka.config.KafkaTestConfig
import com.webtrends.harness.component.kafka.health.ProducerHealth
import com.webtrends.harness.health.{ComponentState, HealthComponent}
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.runner.RunWith
import org.specs2.mutable.SpecificationLike
import org.specs2.runner.JUnitRunner

import scala.concurrent.duration.{Duration, SECONDS}

case class TestRequest(kafkaMessage: KafkaMessage)
case class TestResponse(producerRecord: ProducerRecord[Array[Byte], Array[Byte]])

case class HealthParentTester() extends Actor with ActorLogging {
  override def receive = {
    case ProducerHealth(health) =>
      log.info(s"Health from Producer: $health")
  }
}

class KafkaWriterTester(healthParent: ActorRef) extends KafkaWriter(healthParent) {
  override def receive:Receive = super.receive orElse {
    case TestRequest(message) =>
      sender ! TestResponse(keyedMessage(message))
    case hc: HealthComponent =>
      setHealth(hc)
  }

  override def postStop() = {}
  override def newProducer = null
  override def sendMessages(messages: Seq[KafkaMessage]) = {}
}

@RunWith(classOf[JUnitRunner])
class KafkaWriterSpec extends  SpecificationLike {
  implicit val system = ActorSystem("test", KafkaTestConfig.config)
  val healthTester = system.actorOf(Props[HealthParentTester], "kafka-writer-tester-health")
  val kafkaWriter = system.actorOf(Props(classOf[KafkaWriterTester], healthTester), "kafka-writer-tester")
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

    "write messages and return an ack" in {
      val kafkaMessage = KafkaMessage("topic", msgData)

      probe.send(kafkaWriter, MessagesToWriteWithAck("test_ack", kafkaMessage))

      val ack = probe.expectMsgClass(duration, classOf[MessageWriteAck]).ackId

      ack match {
        case Left(ackString) =>
          ackString mustEqual "test_ack"
        case Right(ex) =>
          throw ex
      }
    }

    "write messages but return error when health is bad" in {
      try {
        probe.send(kafkaWriter, HealthComponent(kafkaWriter.path.name,
          ComponentState.CRITICAL, "Set to CRITICAL for test"))

        val kafkaMessage = KafkaMessage("topic", msgData)
        probe.send(kafkaWriter, MessagesToWriteWithAck("test_ack", kafkaMessage))

        val ack = probe.expectMsgClass(duration, classOf[MessageWriteAck]).ackId

        ack match {
          case Left(ackString) =>
            failure(s"Got $ackString but should have failed")
          case Right(ex) =>
            ex.ackId mustEqual "test_ack"
            success
        }
      } finally {
        probe.send(kafkaWriter, HealthComponent(kafkaWriter.path.name,
          ComponentState.NORMAL, "Writer not experiencing errors"))
      }
    }
  }
}



