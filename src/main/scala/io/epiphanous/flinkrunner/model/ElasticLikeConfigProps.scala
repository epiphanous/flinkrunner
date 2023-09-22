package io.epiphanous.flinkrunner.model

import org.apache.http.HttpHost

import java.net.URL

trait ElasticLikeConfigProps {
  this: {
    val config: FlinkConfig
    def pfx(s: String): String
  } =>

  lazy val index: String = config.getString(pfx("index"))

  lazy val transports: List[HttpHost] =
    config.getStringList(pfx("transports")).map { s =>
      val url      = new URL(if (s.startsWith("http")) s else s"http://$s")
      val hostname = url.getHost
      val port     = if (url.getPort < 0) 9200 else url.getPort
      new HttpHost(hostname, port, url.getProtocol)
    }

  lazy val bulkFlushBackoffType: String =x
    config
      .getStringOpt(pfx("bulk.flush.backoff.type"))
      .map(_.toUpperCase)
      .getOrElse("NONE")

  lazy val bulkFlushBackoffRetries: Int =
    config.getIntOpt(pfx("bulk.flush.backoff.retries")).getOrElse(5)

  lazy val bulkFlushBackoffDelay: Long =
    config.getLongOpt(pfx("bulk.flush.backoff.delay")).getOrElse(1000)

  lazy val bulkFlushMaxActions: Option[Int] =
    config.getIntOpt(pfx("bulk.flush.max.actions"))

  lazy val bulkFlushMaxSizeMb: Option[Int] =
    config.getIntOpt(pfx("bulk.flush.max.size.mb"))

  lazy val bulkFlushIntervalMs: Option[Long] =
    config.getLongOpt(pfx("bulk.flush.interval.ms"))

}
