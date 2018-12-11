package io.epiphanous.flinkrunner.model
import com.typesafe.config.ConfigObject
import io.epiphanous.flinkrunner.model.FlinkConnectorName._

sealed trait SourceConfig extends ConfigToProps {
  def connector: FlinkConnectorName
  def name: String
  def label: String = s"$connector/$name"
}

object SourceConfig {
  def apply(name: String, config: FlinkConfig): SourceConfig = {
    val p = s"sources.$name"
    FlinkConnectorName.withNameInsensitiveOption(config.getString(s"$p.connector")) match {
      case Some(connector) =>
        connector match {
          case Kafka =>
            KafkaSourceConfig(connector, name, config.getString(s"$p.topic"), config.getObjectOption(s"$p.config"))
          case KeyedKafka =>
            KeyedKafkaSourceConfig(connector, name, config.getString(s"$p.topic"), config.getObjectOption(s"$p.config"))
          case Kinesis =>
            KinesisSourceConfig(connector, name, config.getString(s"$p.stream"), config.getObjectOption(s"$p.config"))
          case File =>
            FileSourceConfig(connector, name, config.getString(s"$p.path"), config.getObjectOption(s"$p.config"))
          case Socket =>
            SocketSourceConfig(connector,
                               name,
                               config.getString(s"$p.host"),
                               config.getInt(s"$p.port"),
                               config.getObjectOption(s"$p.config"))
          case Collection =>
            CollectionSourceConfig(connector, name, name, config.getObjectOption(s"$p.config"))
          case other => throw new RuntimeException(s"$other $name connector not valid source (job ${config.jobName}")
        }
      case None => throw new RuntimeException(s"Invalid/missing source connector type for $name (job ${config.jobName}")
    }
  }
}

final case class KafkaSourceConfig(
  connector: FlinkConnectorName = Kafka,
  name: String,
  topic: String,
  config: Option[ConfigObject] = None)
    extends SourceConfig
final case class KeyedKafkaSourceConfig(
  connector: FlinkConnectorName = KeyedKafka,
  name: String,
  topic: String,
  config: Option[ConfigObject] = None)
    extends SourceConfig
final case class KinesisSourceConfig(
  connector: FlinkConnectorName = Kinesis,
  name: String,
  stream: String,
  config: Option[ConfigObject] = None)
    extends SourceConfig
final case class FileSourceConfig(
  connector: FlinkConnectorName = File,
  name: String,
  path: String,
  config: Option[ConfigObject] = None)
    extends SourceConfig
final case class SocketSourceConfig(
  connector: FlinkConnectorName = Socket,
  name: String,
  host: String,
  port: Int,
  config: Option[ConfigObject] = None)
    extends SourceConfig
final case class CollectionSourceConfig(
  connector: FlinkConnectorName = Collection,
  name: String,
  topic: String,
  config: Option[ConfigObject] = None)
    extends SourceConfig
