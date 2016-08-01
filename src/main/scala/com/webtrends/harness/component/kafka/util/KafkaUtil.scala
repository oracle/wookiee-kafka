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

package com.webtrends.harness.component.kafka.util

import java.util.Properties

import com.typesafe.config.{Config, ConfigValueType}
import com.webtrends.harness.component.kafka.KafkaManager
import kafka.api.{PartitionOffsetRequestInfo, _}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import org.slf4j.LoggerFactory

object KafkaUtil {
  val log = LoggerFactory.getLogger(classOf[KafkaManager])

  def getFetchRequest(clientName: String, topic: String, part: Int, offset: Long, fetchSize: Int): FetchRequest = {
    val req: FetchRequest = new FetchRequestBuilder().clientId(clientName).addFetch(topic, part, offset, fetchSize).build()
    req
  }

  def getDesiredAvailableOffset(consumer: SimpleConsumer, topic: String, partition: Int, desiredStartOffset: Long, clientName: String): Long = {

    val smallest = getSmallestAvailableOffset(consumer, topic, partition)
    val largest = getLargestAvailableOffset(consumer, topic, partition)

    log.warn(s"$topic:$partition Desired offset $desiredStartOffset out of range. Min: $smallest max: $largest")
    if (desiredStartOffset > smallest && desiredStartOffset <= largest) {
      var foundOffset = false
      log.warn(s"$topic:$partition Desired offset $desiredStartOffset seems to be an empty segment, finding next safe area")
      var currentOffset = desiredStartOffset
      val onePercent = Math.max(Math.round((largest - desiredStartOffset) * .01), 1)
      while (!foundOffset && currentOffset < largest) {
        val req = new FetchRequestBuilder()
          .clientId(clientName)
          .addFetch(topic, partition, currentOffset, 1)
          .build()
        val fetchResponse = consumer.fetch(req)
        if (!fetchResponse.hasError) {
          foundOffset = true
        } else currentOffset += onePercent
      }
      log.warn(s"$topic:$partition Was safe area found: $foundOffset, new offset: $currentOffset")
      Math.min(currentOffset, largest)
    } else if (desiredStartOffset > largest) {
      log.warn(s"$topic:$partition Offset $desiredStartOffset after range in kafka, starting from latest offset $largest")
      largest
    }
    else {
      log.warn(s"$topic:$partition Offset $desiredStartOffset before range in kafka, starting from earliest offset $smallest")
      smallest
    }
  }

  def getSmallestAvailableOffset(consumer: SimpleConsumer, topic: String, partition: Int): Long = {
    val topicAndPartition: TopicAndPartition = new TopicAndPartition(topic, partition)

    val endOffsetRequest = OffsetRequest(
      Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1)),
      replicaId = 0) //TODO: determine replicaId, if needed?

    val endResponse: OffsetResponse = consumer.getOffsetsBefore(endOffsetRequest)
    if (endResponse.hasError) {
      throw new IllegalStateException("Kafka response error getting earliest offsets" )
    }
    val endOffsets: Seq[Long] = endResponse.partitionErrorAndOffsets(topicAndPartition).offsets
    if (endOffsets.length != 1) {
      throw new IllegalStateException(s"Expect one earliest offset but got [${endOffsets.length}]")
    }
    endOffsets.head
  }

  def getLargestAvailableOffset(consumer: SimpleConsumer, topic: String, partition: Int): Long = {
    val topicAndPartition: TopicAndPartition = new TopicAndPartition(topic, partition)

    val startOffsetRequest = OffsetRequest(
      Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)),
      replicaId = 0) //TODO: determine replicaId, if needed?

    val startResponse: OffsetResponse = consumer.getOffsetsBefore(startOffsetRequest)
    if (startResponse.hasError) {
      throw new IllegalStateException("Kafka response error getting earliest offsets" )
    }
    val startOffsets: Seq[Long] = startResponse.partitionErrorAndOffsets(topicAndPartition).offsets
    if (startOffsets.length != 1) {
      throw new IllegalStateException(s"Expect one earliest offset but got [${startOffsets.length}]")
    }

    startOffsets.head
  }

  def configToProps(config: Config): Properties = {
    val props: Properties = new Properties
    import scala.collection.JavaConversions._
    config.entrySet.foreach { entry =>
      entry.getValue.valueType match {
        case ConfigValueType.STRING =>
          props.put(entry.getKey, config.getString(entry.getKey))
        case ConfigValueType.NUMBER =>
          props.put(entry.getKey, config.getNumber(entry.getKey).toString)
        case _ =>
        // Ignore other types
      }
    }
    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer")
    props
  }
}
