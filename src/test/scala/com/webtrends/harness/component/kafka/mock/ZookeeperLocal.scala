package com.webtrends.harness.component.kafka.mock

import java.io.IOException
import java.util.Properties

import org.apache.zookeeper.server.{ServerConfig, ZooKeeperServerMain}
import org.apache.zookeeper.server.quorum.QuorumPeerConfig

class ZookeeperLocal {
  val props: Properties = new Properties()
  props.load(getClass.getResourceAsStream("/zkLocal.properties"))

  val quorumConfiguration = new QuorumPeerConfig
  try
    quorumConfiguration.parseProperties(props)
  catch {
    case e: Exception =>
      throw new RuntimeException(e)
  }

  val zooKeeperServer = new ZooKeeperServerMain
  val configuration = new ServerConfig
  configuration.readFrom(quorumConfiguration)

  new Thread() {
    override def run() = {
      try
        zooKeeperServer.runFromConfig(configuration)
      catch {
        case e: IOException =>
          System.out.println("ZooKeeper Failed")
          e.printStackTrace(System.err)
      }
    }
  }.start()
}
