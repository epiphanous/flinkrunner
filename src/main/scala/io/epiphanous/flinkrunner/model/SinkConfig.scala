package io.epiphanous.flinkrunner.model

import com.typesafe.config.ConfigObject
import io.epiphanous.flinkrunner.model.FlinkConnectorName._

sealed trait SinkConfig extends ConfigToProps {
  def connector: FlinkConnectorName
  def name: String
  def label: String = s"$connector/$name"
}

object SinkConfig {
  def apply(name: String, config: FlinkConfig): SinkConfig = {
    val p = s"sinks.$name"
    FlinkConnectorName.withNameInsensitiveOption(config.getString(s"$p.connector")) match {
      case Some(connector) =>
        connector match {
          case Kafka =>
            KafkaSinkConfig(connector, name, config.getString(s"$p.topic"), config.getObjectOption(s"$p.config"))
          case KeyedKafka =>
            KeyedKafkaSinkConfig(connector, name, config.getString(s"$p.topic"), config.getObjectOption(s"$p.config"))
          case Kinesis =>
            KinesisSinkConfig(connector, name, config.getString(s"$p.stream"), config.getObjectOption(s"$p.config"))
          case File =>
            FileSinkConfig(connector, name, config.getString(s"$p.path"), config.getObjectOption(s"$p.config"))
          case Socket =>
            SocketSinkConfig(connector,
                             name,
                             config.getString(s"$p.host"),
                             config.getInt(s"$p.port"),
                             config.getObjectOption(s"$p.config"))
          case Jdbc =>
            JdbcSinkConfig(connector, name, config.getString(s"$p.query"), config.getObjectOption(s"$p.config"))
          case other => throw new RuntimeException(s"$other $name connector not valid sink (job ${config.jobName}")

        }
      case None => throw new RuntimeException(s"Invalid/missing sink connector type for $name (job ${config.jobName}")
    }
  }
}

final case class KafkaSinkConfig(
  connector: FlinkConnectorName = Kafka,
  name: String,
  topic: String,
  config: Option[ConfigObject] = None)
    extends SinkConfig
final case class KeyedKafkaSinkConfig(
  connector: FlinkConnectorName = Kafka,
  name: String,
  topic: String,
  config: Option[ConfigObject] = None)
    extends SinkConfig
final case class KinesisSinkConfig(
  connector: FlinkConnectorName = Kinesis,
  name: String,
  stream: String,
  config: Option[ConfigObject] = None)
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
  config: Option[ConfigObject] = None)
    extends SinkConfig
final case class SocketSinkConfig(
  connector: FlinkConnectorName = Socket,
  name: String,
  host: String,
  port: Int,
  config: Option[ConfigObject] = None)
    extends SinkConfig
final case class JdbcSinkConfig(
  connector: FlinkConnectorName = Jdbc,
  name: String,
  query: String,
  config: Option[ConfigObject] = None)
    extends SinkConfig
