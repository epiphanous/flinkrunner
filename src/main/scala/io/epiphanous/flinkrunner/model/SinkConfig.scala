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
  def apply[ADT <: FlinkEvent](
      name: String,
      config: FlinkConfig[ADT]): SinkConfig = {
    val p = s"sinks.$name"
    FlinkConnectorName.withNameInsensitiveOption(
      config.getString(s"$p.connector")
    ) match {
      case Some(connector) =>
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
            FileSinkConfig(
              connector,
              name,
              config.getString(s"$p.path"),
              config.getProperties(s"$p.config")
            )
          case Socket            =>
            SocketSinkConfig(
              connector,
              name,
              config.getString(s"$p.host"),
              config.getInt(s"$p.port"),
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

          case RabbitMQ =>
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
          case other    =>
            throw new RuntimeException(
              s"$other $name connector not valid sink (job ${config.jobName}"
            )

        }
      case None            =>
        throw new RuntimeException(
          s"Invalid/missing sink connector type for $name (job ${config.jobName}"
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

//Long("bucket.check.interval")
//String("bucket.assigner")
//String("bucket.assigner.datetime.format")
//String("encoder.format")
//String("bucket.rolling.policy")
//Long("bucket.rolling.policy.inactivity.interval")
//Long("bucket.rolling.policy.max.part.size")
//Long("bucket.rolling.policy.rollover.interval")

final case class FileSinkConfig(
    connector: FlinkConnectorName = File,
    name: String,
    path: String,
    properties: Properties)
    extends SinkConfig

final case class SocketSinkConfig(
    connector: FlinkConnectorName = Socket,
    name: String,
    host: String,
    port: Int,
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
