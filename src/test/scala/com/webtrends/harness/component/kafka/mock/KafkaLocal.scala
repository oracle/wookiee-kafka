package com.webtrends.harness.component.kafka.mock

import java.io.File

import kafka.server.KafkaConfig
import kafka.server.KafkaServerStartable
import java.util.Properties

class KafkaLocal {
  val kafkaProperties: Properties = new Properties()
  kafkaProperties.load(getClass.getResourceAsStream("/kafkaLocal.properties"))
  val kafkaConfig = new KafkaConfig(kafkaProperties)
  val logDirs = kafkaProperties.getProperty("log.dirs")
  new File(logDirs + "/local-topic")
  //start local zookeeper
  println("Starting local zookeeper...")
  val zookeeper = new ZookeeperLocal()

  //start local kafka broker
  println("Starting local kafka broker...")
  val kafka = KafkaServerStartable.fromProps(kafkaProperties)
  kafka.startup()
  println("Local Kafka Up, Ready to Mock")

  def stop() = { //stop kafka broker
    System.out.println("stopping kafka...")
    kafka.shutdown()
    System.out.println("done")
  }
}
