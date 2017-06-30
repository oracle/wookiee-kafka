package com.webtrends.harness.component.kafka.mock

import kafka.server.KafkaConfig
import kafka.server.KafkaServerStartable
import java.io.IOException

class KafkaLocal {


  import kafka.server.KafkaConfig
  import kafka.server.KafkaServerStartable

  val kafkaConfig = new KafkaConfig(kafkaProperties)

  //start local zookeeper
  println("Starting local zookeeper...")
  val zookeeper = new ZookeeperLocal()

  //start local kafka broker
  println("Starting local kafka broker...")
  kafka = new KafkaServerStartable(kafkaConfig)
  kafka.startup
  println("Local Kafka Up, Ready to Mock")
  var kafka = null
  var zookeeper = null




  def stop() = { //stop kafka broker
    System.out.println("stopping kafka...")
    kafka.shutdown()
    System.out.println("done")
  }
}
