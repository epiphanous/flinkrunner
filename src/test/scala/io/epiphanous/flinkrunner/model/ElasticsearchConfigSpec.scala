package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.PropSpec
import org.apache.http.HttpHost

import java.net.URL
import scala.collection.JavaConverters._

class ElasticsearchConfigSpec extends PropSpec {
  def getHosts(transports: List[String]) = transports.map { s =>
    val url      = new URL(if (s.startsWith("http")) s else s"http://$s")
    val hostname = url.getHost
    val port     = if (url.getPort < 0) 9200 else url.getPort
    new HttpHost(hostname, port, url.getProtocol)
  }.asJava

  property("transports config with protocol") {
    println(getHosts(List("https://elastic:9200")))
  }
  property("transports config without protocol") {
    println(getHosts(List("elastic:9200")))
  }
}
