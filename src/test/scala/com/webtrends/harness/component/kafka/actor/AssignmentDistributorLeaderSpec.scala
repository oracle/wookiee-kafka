package com.webtrends.harness.component.kafka.actor

import com.webtrends.harness.component.kafka.actor.AssignmentDistributorLeader.PartitionAssignment
import com.webtrends.harness.health.ComponentState
import org.junit.runner.RunWith
import akka.actor._
import akka.testkit.TestProbe
import com.webtrends.harness.component.kafka.KafkaConsumerCoordinator.{BroadcastToWorkers, TopicPartitionResp}
import com.webtrends.harness.component.kafka.actor.AssignmentDistributorLeader
import com.webtrends.harness.component.kafka.actor.KafkaTopicManager.TopicPartitionReq
import com.webtrends.harness.component.kafka.config.KafkaTestConfig
import com.webtrends.harness.health.{ComponentState, HealthComponent}
import com.webtrends.harness.service.messages.CheckHealth
import net.liftweb.json.Serialization
import org.junit.runner.RunWith
import org.slf4j.LoggerFactory
import org.specs2.mutable.SpecificationLike
import org.specs2.runner.JUnitRunner
import org.specs2.time.NoTimeConversions

import scala.collection.immutable._
import scala.concurrent.duration._


@RunWith(classOf[JUnitRunner])
class AssignmentDistributorLeaderSpec extends SpecificationLike with NoTimeConversions {
  val hosts = List("node1", "node2", "node3")
  val partitions = TreeSet[PartitionAssignment](
    PartitionAssignment("topic1", 0, "cluster1", "leader1"),
    PartitionAssignment("topic1", 1, "cluster1", "leader1"),
    PartitionAssignment("topic1", 2, "cluster1", "leader1"),
    PartitionAssignment("topic1", 3, "cluster1", "leader1"),

    PartitionAssignment("topic1", 0, "cluster2", "leader1"),
    PartitionAssignment("topic1", 1, "cluster2", "leader1"),
    PartitionAssignment("topic1", 2, "cluster2", "leader1"),
    PartitionAssignment("topic1", 3, "cluster2", "leader1"),

    PartitionAssignment("topic2", 0, "cluster3", "leader1"),
    PartitionAssignment("topic2", 1, "cluster3", "leader1"),
    PartitionAssignment("topic2", 2, "cluster3", "leader1"),
    PartitionAssignment("topic2", 3, "cluster3", "leader1"),
    PartitionAssignment("topic2", 4, "cluster3", "leader1"),
    PartitionAssignment("topic2", 5, "cluster3", "leader1")
  )(Ordering.by[PartitionAssignment, String](a => a.topic + a.cluster + a.partition))


  val assignmentInfo = AssignmentDistributorLeader.DistributeAssignments(hosts,
    TopicPartitionResp(partitions))


  "AssignmentDistributorLeader" should {
    "distribute topics and partitions evenly" in {
      val assignments = AssignmentDistributorLeader.getAssignments(assignmentInfo)

      assignments("node1").size must beEqualTo(5)
      assignments("node2").size must beEqualTo(5)
      assignments("node3").size must beEqualTo(4)

      assignments("node1").count(a => a.topic == "topic1" && a.cluster == "cluster1") must beEqualTo(2)
      assignments("node2").count(a => a.topic == "topic1" && a.cluster == "cluster1") must beEqualTo(1)
      assignments("node3").count(a => a.topic == "topic1" && a.cluster == "cluster1") must beEqualTo(1)

      assignments("node1").count(a => a.topic == "topic1" && a.cluster == "cluster2") must beEqualTo(1)
      assignments("node2").count(a => a.topic == "topic1" && a.cluster == "cluster2") must beEqualTo(2)
      assignments("node3").count(a => a.topic == "topic1" && a.cluster == "cluster2") must beEqualTo(1)
      
      assignments("node1").count(a => a.topic == "topic2" && a.cluster == "cluster3") must beEqualTo(2)
      assignments("node2").count(a => a.topic == "topic2" && a.cluster == "cluster3") must beEqualTo(2)
      assignments("node3").count(a => a.topic == "topic2" && a.cluster == "cluster3") must beEqualTo(2)
    }
  }
}
