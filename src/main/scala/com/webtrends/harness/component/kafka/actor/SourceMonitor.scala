package com.webtrends.harness.component.kafka.actor

import akka.actor.ActorRef
import com.webtrends.harness.app.HActor
import com.webtrends.harness.component.kafka.actor.KafkaTopicManager.DownSources
import com.webtrends.harness.component.kafka.util.KafkaSettings
import com.webtrends.harness.component.metrics.MetricsAdapter
import com.webtrends.harness.component.metrics.metrictype.Counter
import org.apache.http.client.config.RequestConfig
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
case class LastMessage()

class SourceMonitor(component: String) extends HActor with KafkaSettings with MetricsAdapter {
  val nagiosServer = Try { kafkaConfig.getString("nagios-host") } getOrElse "http://nagios:8080/host/"
  val downSources = mutable.Set.empty[String]
  // Calls to nagios should timeout if the API becomes despondent
  val nagiosConnectTimeout = Try { kafkaConfig.getInt("nagios-timeout-millis") } getOrElse 5000
  val requestBuilder = RequestConfig.custom().setConnectTimeout(nagiosConnectTimeout)
    .setConnectionRequestTimeout(nagiosConnectTimeout).build()

  val nagiosSourceChecks = Counter(s"nagios.source.checks.$component")

  def waitingOnHostList: Receive = {
    case HostList(hosts) =>
      self ! LastMessage()
      context.become(processLastHostList(hosts.toSet, sender()))
  }

  def processLastHostList(lastHostList: Set[String], itsSender: ActorRef): Receive = {
    case HostList(hosts) => context.become(processLastHostList(hosts.toSet, sender()))
    case LastMessage() =>
      itsSender ! DownSources(lastHostList.filter(host => !hostIsGreen(host.split('.')(0))))
      context.become(waitingOnHostList)
  }

  override def receive: Receive = waitingOnHostList

  def hostIsGreen(host: String): Boolean = {
    try {
      val response = getRestContent(s"$nagiosServer$host")
      response match {
        case Some(resp) =>
          nagiosSourceChecks.incr
          val parsed = JSON.parseRaw(resp).get.asInstanceOf[JSONObject].obj("content").asInstanceOf[JSONObject]
          if (parsed.obj("scheduled_downtime_depth") != "0") downSources.add(host) else downSources.remove(host)
          parsed.obj("scheduled_downtime_depth") == "0"
        case None =>
          log.warn(s"$component: Did not get a nagios response for $host, using last status of ${downSources.contains(host)}")
          !downSources.contains(host) // Were we able to get through to nagios last time and see the host was down
      }
    } catch {
      case e: NoSuchElementException =>
        log.error(e, s"$component: Error parsing nagios response for host $host")
        !downSources.contains(host) // Nagios status call returns something unexpected
      case e: Exception =>
        log.error(e, s"$component: Error getting nagios response for host $host")
        !downSources.contains(host)
    }
  }

  // Returns the text content from a REST URL. Returns a None if there is a problem.
  def getRestContent(url:String): Option[String] = {
    var content: Option[String] = None
    val httpClient = HttpClientBuilder.create().setDefaultRequestConfig(requestBuilder).build()
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
