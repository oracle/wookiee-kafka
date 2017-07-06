package com.webtrends.harness.component.kafka.actor

import java.io.IOException
import java.nio.charset.{Charset, StandardCharsets}

import akka.actor.{Actor, ActorRef, Props}
import akka.util.Timeout
import com.webtrends.harness.component.kafka.actor.KafkaTopicManager.BrokerSpec
import com.webtrends.harness.component.kafka.health.ZKHealthState
import com.webtrends.harness.component.kafka.util.KafkaSettings
import com.webtrends.harness.component.zookeeper.ZookeeperAdapter
import com.webtrends.harness.logging.ActorLoggingAdapter
import kafka.api.GroupCoordinatorRequest
import kafka.common.{ErrorMapping, OffsetAndMetadata, OffsetMetadata, TopicAndPartition}
import kafka.javaapi._
import kafka.network.BlockingChannel

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.duration._

object OffsetManager {
  def props(appRoot: String, timeout: Timeout = 5 seconds): Props =
    Props(new OffsetManager(appRoot, timeout))

  case class OffsetData(data: Array[Byte], offset: Long) {
    def asString(charset:Charset = StandardCharsets.UTF_8) = new String(data, charset)
  }

  case class GetOffsetData(topic: String, cluster: String, partition: Int)
  case class StoreOffsetData(topic: String, cluster: String, partition: Int, offsetData: OffsetData)

  case class OffsetDataResponse(data: Either[OffsetData, Throwable])
}

class OffsetManager(appRoot: String, timeout:Timeout) extends Actor
  with ActorLoggingAdapter with ZookeeperAdapter with KafkaSettings {
  import OffsetManager._
  implicit val implicitTimeout = timeout

  val brokers = mutable.HashMap[String, (BlockingChannel, Int, BrokerSpec)]()

  override def preStart(): Unit = {
    connect()
    super.preStart()
  }

  def connect(): Unit = {
    try {
      kafkaSources.foreach {
        case (_, broker) if !brokers.contains(broker.cluster) => refreshBroker(broker)
      }
    } catch {
      case e: IOException =>
        log.error(s"Exception getting metadata", e)
    }
  }

  def refreshBroker(broker: BrokerSpec): Unit = {
    log.info(s"Creating/Refreshing Broker for cluster: ${broker.cluster}")
    val oldBroker = brokers.remove(broker.cluster)

    oldBroker.foreach(_._1.disconnect())
    val cId = oldBroker.map(_._2).getOrElse(0)

    var channel = new BlockingChannel(broker.host, broker.port,
      BlockingChannel.UseDefaultBufferSize,
      BlockingChannel.UseDefaultBufferSize,
      5000 /* read timeout in millis */)
    channel.connect()

    channel.send(new GroupCoordinatorRequest(pod, GroupCoordinatorRequest.CurrentVersion, cId, clientId))
    val metadataResponse = GroupCoordinatorResponse.readFrom(channel.receive().payload())

    if (metadataResponse.errorCode == ErrorMapping.NoError) {
      val offsetManager = metadataResponse.coordinator
      // if the coordinator is different, from the above channel's host then reconnect
      if (offsetManager.host != broker.host) {
        channel.disconnect()
        channel = new BlockingChannel(offsetManager.host, offsetManager.port,
          BlockingChannel.UseDefaultBufferSize,
          BlockingChannel.UseDefaultBufferSize,
          5000 /* read timeout in millis */)
        channel.connect()
      }
    } else {
      // retry (after backoff)
      log.error(s"Error getting metadata ${metadataResponse.toString()}")
    }
    brokers.put(broker.cluster, (channel, cId + 1, broker))
  }

  def receive: Receive = {
    case msg: GetOffsetData => retrieveOffsetState(msg)
    case msg: StoreOffsetData => storeOffsetState(msg)
    case msg: BrokerSpec => refreshBroker(msg)
  }

  /**
   * Get the state from zk and send back offset data response
   */
  def retrieveOffsetState(req: GetOffsetData) = {
    val originalSender = sender()

    brokers.get(req.cluster) match {
      case Some(brokerInfo) =>
        val channel = brokerInfo._1
        val broker = brokerInfo._3
        try {
          val partitions = List(TopicAndPartition(req.topic, req.partition))
          val fetchRequest = new OffsetFetchRequest(pod, partitions, 1, brokerInfo._2, clientId)

          channel.send(fetchRequest.underlying)
          val fetchResponse = OffsetFetchResponse.readFrom(channel.receive().payload())
          val result = fetchResponse.offsets.get(partitions.head)

          val offsetFetchErrorCode = result.error
          if (offsetFetchErrorCode == ErrorMapping.NotCoordinatorForConsumerCode) {
            errorInRetrieval(broker, s"Coordinator switched for cluster ${broker.cluster}", originalSender)
          } else if (offsetFetchErrorCode == ErrorMapping.OffsetsLoadInProgressCode) {
            errorInRetrieval(broker, s"Offset load in progress for cluster ${broker.cluster}", originalSender)
          } else {
            val offData = OffsetData(result.metadata.getBytes, result.offset)
            originalSender ! OffsetDataResponse(Left(offData))
            context.parent ! healthy(req.cluster)
          }
        } catch {
          case ex: IOException =>
            errorInRetrieval(broker, s"Cluster ${broker.cluster} failed with ${ex.getMessage}", originalSender)
        }
      case None =>
        log.error(s"No broker found for cluster ${req.cluster}, making one")
        originalSender ! OffsetDataResponse(Left(OffsetData(Array(), 0L)))
        kafkaSources.get(req.cluster).foreach(refreshBroker)
    }
  }

  def errorInRetrieval(broker: BrokerSpec, message: String, sender: ActorRef): Unit = {
    log.error(message)
    sender ! OffsetDataResponse(Right(new IllegalStateException(message)))
    context.parent ! unhealthy(broker.cluster)
    refreshBroker(broker)
  }

  def storeOffsetState(req: StoreOffsetData, create: Boolean = false): Unit = {
    val now = System.currentTimeMillis()
    val partitions = Map(TopicAndPartition(req.topic, req.partition) ->
      new OffsetAndMetadata(OffsetMetadata(req.offsetData.offset, new String(req.offsetData.data)), now))

    brokers.get(req.cluster) match {
      case Some(brokerInfo) =>
        val commitRequest = new OffsetCommitRequest(pod, partitions, brokerInfo._2, clientId, 1)
        val channel = brokerInfo._1

        try {
          channel.send(commitRequest.underlying)
          val commitResponse = OffsetCommitResponse.readFrom(channel.receive().payload())

          if (commitResponse.hasError) {
            commitResponse.errors.values().toArray.foreach { partitionErrorCode =>
              errorInRetrieval(brokerInfo._3, s"Error $partitionErrorCode on cluster ${req.cluster}", sender())
            }
          } else {
            sender() ! OffsetDataResponse(Left(OffsetData(req.offsetData.data, req.offsetData.offset)))
            context.parent ! healthy(req.cluster)
          }
        } catch {
          case ex: IOException =>
            errorInRetrieval(brokerInfo._3, s"Error on cluster ${req.cluster}: ${ex.getMessage}", sender())
        }
      case None =>
        log.warn(s"Write error: No broker found for cluster ${req.cluster}, making one")
        sender() ! OffsetDataResponse(Right(new IllegalStateException(s"No broker found for cluster ${req.cluster}")))
        kafkaSources.get(req.cluster).foreach(refreshBroker)
    }
  }

  private def unhealthy(path: String) =
    ZKHealthState(path, healthy = false, s"Failed to fetch offset state for cluster $path from Kafka")

  private def healthy(path: String) =
    ZKHealthState(path, healthy = true, s"Successfully fetched state for cluster $path from Kafka")
}
