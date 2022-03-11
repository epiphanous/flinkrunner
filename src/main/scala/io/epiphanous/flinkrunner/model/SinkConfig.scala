package io.epiphanous.flinkrunner.model

import com.google.common.collect.Maps
import io.epiphanous.flinkrunner.model.FlinkConnectorName._

import java.util
import java.util.Properties

sealed trait SinkConfig {
  def connector: FlinkConnectorName

  def name: String

  def label: String = s"$connector/$name"

  def properties: Properties

  def propertiesMap: util.HashMap[String, String] =
    Maps.newHashMap(Maps.fromProperties(properties))
}

object SinkConfig {
  def apply(name: String, config: FlinkConfig): SinkConfig = {
    val p = s"sinks.$name"

    val connector = FlinkConnectorName
      .fromSinkName(
        name,
        config.jobName,
        config.getStringOpt(s"$p.connector")
      )

    connector match {
      case Kafka             =>
        KafkaSinkConfig(
          connector,
          name,
          config.getString(s"$p.topic"),
          config.getBoolean(s"$p.isKeyed"),
          config.getProperties(s"$p.config")
        )
      case Kinesis           =>
        KinesisSinkConfig(
          connector,
          name,
          config.getString(s"$p.stream"),
          config.getProperties(s"$p.config")
        )
      case File              =>
        val format = config.getStringOpt(s"$p.format").getOrElse("row")
        FileSinkConfig(
          connector,
          name,
          config.getString(s"$p.path"),
          format.equalsIgnoreCase("bulk"),
          format,
          config.getProperties(s"$p.config")
        )
      case Socket            =>
        SocketSinkConfig(
          connector,
          name,
          config.getString(s"$p.host"),
          config.getInt(s"$p.port"),
          config.getIntOpt(s"$p.max.retries"),
          config.getBooleanOpt(s"$p.auto.flush"),
          config.getProperties(s"$p.config")
        )
      case Jdbc              =>
        JdbcSinkConfig(
          connector,
          name,
          config.getString(s"$p.url"),
          config.getString(s"$p.query"),
          config.getProperties(s"$p.config")
        )
      case CassandraSink     =>
        CassandraSinkConfig(
          connector,
          name,
          config.getString(s"$p.host"),
          config.getString(s"$p.query"),
          config.getProperties(s"$p.config")
        )
      case ElasticsearchSink =>
        ElasticsearchSinkConfig(
          connector,
          name,
          config.getStringList(s"$p.transports"),
          config.getString(s"$p.index"),
          config.getString(s"$p.type"),
          config.getProperties(s"$p.config")
        )
      case RabbitMQ          =>
        val c   = config.getProperties(s"$p.config")
        val uri = config.getString(s"$p.uri")
        RabbitMQSinkConfig(
          connector,
          name,
          uri,
          config.getBoolean(s"$p.use.correlation.id"),
          RabbitMQConnectionInfo(uri, c),
          Option(c.getProperty("queue")),
          c
        )
      case _                 =>
        throw new RuntimeException(
          s"Don't know how to configure ${connector.entryName} sink connector $name (job ${config.jobName}"
        )
    }
  }
}

final case class KafkaSinkConfig(
    connector: FlinkConnectorName = Kafka,
    name: String,
    topic: String,
    isKeyed: Boolean,
    properties: Properties)
    extends SinkConfig

final case class KinesisSinkConfig(
    connector: FlinkConnectorName = Kinesis,
    name: String,
    stream: String,
    properties: Properties)
    extends SinkConfig

final case class FileSinkConfig(
    connector: FlinkConnectorName = File,
    name: String,
    path: String,
    isBulk: Boolean,
    format: String,
    properties: Properties)
    extends SinkConfig

final case class SocketSinkConfig(
    connector: FlinkConnectorName = Socket,
    name: String,
    host: String,
    port: Int,
    maxRetries: Option[Int] = None,
    autoFlush: Option[Boolean] = None,
    properties: Properties)
    extends SinkConfig

final case class JdbcSinkConfig(
    connector: FlinkConnectorName = Jdbc,
    name: String,
    url: String,
    query: String,
    properties: Properties)
    extends SinkConfig

final case class CassandraSinkConfig(
    connector: FlinkConnectorName = CassandraSink,
    name: String,
    host: String,
    query: String,
    properties: Properties)
    extends SinkConfig

final case class ElasticsearchSinkConfig(
    connector: FlinkConnectorName = ElasticsearchSink,
    name: String,
    transports: List[String],
    index: String,
    `type`: String,
    properties: Properties)
    extends SinkConfig

final case class RabbitMQSinkConfig(
    connector: FlinkConnectorName = RabbitMQ,
    name: String,
    uri: String,
    useCorrelationId: Boolean,
    connectionInfo: RabbitMQConnectionInfo,
    queue: Option[String],
    properties: Properties)
    extends SinkConfig
