package com.webtrends.harness.component.kafka.actor

import com.webtrends.harness.app.HActor
import com.webtrends.harness.component.kafka.actor.KafkaTopicManager.DownSources
import com.webtrends.harness.component.kafka.util.KafkaSettings
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder

import scala.io.Source
import scala.util.Try
import scala.util.parsing.json.{JSON, JSONObject}
import scala.collection.mutable

/**
 * Allows one to setup monitoring on any source nodes to watch for scheduled downtime and put workers into a NORMAL state
 * instead of DEGRADED/CRITICAL when it is expected for them to go down
 */
case class HostList(hosts: List[String])

class SourceMonitor extends HActor with KafkaSettings {
  val nagiosServer = Try { kafkaConfig.getString("nagios-host") } getOrElse "http://nagios:8080/host/"
  val downSources = mutable.Set.empty[String]

  override def receive: Receive = {
    case HostList(hosts) => sender ! DownSources(hosts.toSet.filter(host => !hostIsGreen(host.split('.')(0))))
  }

  def hostIsGreen(host: String): Boolean = {
    try {
      val response = getRestContent(s"$nagiosServer$host")
      response match {
        case Some(resp) =>
          val parsed = JSON.parseRaw(resp).get.asInstanceOf[JSONObject].obj("content").asInstanceOf[JSONObject]
          if (parsed.obj("scheduled_downtime_depth") != "0") downSources.add(host) else downSources.remove(host)
          parsed.obj("scheduled_downtime_depth") == "0"
        case None =>
          log.warn(s"Did not get a nagios response for $host, using last status of ${downSources.contains(host)}")
          !downSources.contains(host) // Were we able to get through to nagios last time and see the host was down
      }
    } catch {
      case e: NoSuchElementException => !downSources.contains(host) // Nagios status call returns something unexpected
      case e: Exception =>
        log.error(e, s"Error parsing nagios response for host $host")
        !downSources.contains(host)
    }
  }

  // Returns the text content from a REST URL. Returns a None if there is a problem.
  def getRestContent(url:String): Option[String] = {
    var content: Option[String] = None
    val httpClient = HttpClientBuilder.create().build()
    try {
      val httpResponse = httpClient.execute(new HttpGet(url))
      val entity = httpResponse.getEntity

      if (entity != null) {
        val inputStream = entity.getContent
        content = Some(Source.fromInputStream(inputStream).getLines().mkString)
        inputStream.close()
      }
    } finally {
      httpClient.close()
    }
    content
  }
}
