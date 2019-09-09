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

package com.webtrends.harness.component.kafka.health

import java.util.concurrent.TimeUnit

import com.webtrends.harness.component.kafka.actor.KafkaWriter
import com.webtrends.harness.component.kafka.actor.KafkaWriter.{KafkaMessage, MessageToWrite}
import com.webtrends.harness.health.ComponentState._
import com.webtrends.harness.health.{ComponentState, HealthComponent}
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.duration._
import scala.concurrent.{Future, blocking}
import scala.util.Try

case class ProducerHealth(healthComponent: HealthComponent)
case object KafkaHealthRequest

trait KafkaWriterHealthCheck { this: KafkaWriter =>
  var currentHealth: HealthComponent = HealthComponent(self.path.name, NORMAL, "Writer not experiencing errors")

  var inProcessSend: Option[java.util.concurrent.Future[RecordMetadata]] = None

  // Amount of time we allow an async produce to run for before we consider it unhealthy
  // We need to allow the entire linger time since the production may not even start until this time has passed
  // plus some overhead for the production to actually occur.
  lazy val maxSendTimeMillis = Try { kafkaConfig.getConfig("producer").getInt("linger.ms") }.getOrElse(5000) +
    Try { kafkaConfig.getConfig("producer").getInt("produce-timeout-millis") }.getOrElse(10000)

  // Schedule a regular send to detect changes even if no data is flowing through
  val healthMessage = MessageToWrite(KafkaMessage("producer_health", "Healthy".getBytes("utf-8"), None, None))
  val scheduledHealthProduce = if (Try(context.system.settings.config.getBoolean("wookiee-kafka.producer-scheduled-check")).getOrElse(true))
    context.system.scheduler.schedule(0 seconds, 5 seconds, self, healthMessage)(scala.concurrent.ExecutionContext.Implicits.global)
  else null

  def setHealth(hc: HealthComponent): Unit = {
    if (hc.state != currentHealth.state) {
      currentHealth = hc
      healthParent ! ProducerHealth(currentHealth)
    }
  }

  override def preStart(): Unit = {
    if (healthParent != null) healthParent ! ProducerHealth(currentHealth)
  }

  override def postStop(): Unit = {
    if (scheduledHealthProduce != null) scheduledHealthProduce.cancel()
  }

  // The Kafka API makes it difficult to identify the target kafka server being down. A provided callback will not
  // be called and calling get on the returned java Future will block indefinitely.
  //
  // Addressing this by wrapping the returned java Future and into a scala Future and blocking with defined timeout.
  // If it completes successfully, and we don't have a normal health, set it to NORMAL
  // If it throws an exception or times out, set our health to alert error
  //
  // To limit performance impact, we are only monitoring a single send at at ime
  def monitorSendHealth(sendFuture: java.util.concurrent.Future[RecordMetadata]) {
    inProcessSend match {
      case Some(f) => // Don't do anything, already monitoring a send
      case None =>
        inProcessSend = Some(sendFuture)
        Future {
          blocking {
            try {
              sendFuture.get(maxSendTimeMillis, TimeUnit.MILLISECONDS)
              setHealth(HealthComponent(self.path.name, ComponentState.NORMAL, "Write successful"))
            }
            catch {
              case ex: Exception =>
                log.error("Unable to produce data. Marking producer as unhealthy", ex)
                setHealth(HealthComponent(self.path.name, errorState, ex.getMessage))
                throw ex
            }
            finally {
              inProcessSend = None // Note that this may be executed in a different thread than the current message being processed
            }
          }
        }(scala.concurrent.ExecutionContext.Implicits.global)
    }
  }
}