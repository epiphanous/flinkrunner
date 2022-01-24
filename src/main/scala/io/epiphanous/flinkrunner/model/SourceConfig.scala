package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.model.FlinkConnectorName._

import java.util.Properties
import scala.util.Try

sealed trait SourceConfig {
  def connector: FlinkConnectorName

  def name: String

  def label: String = s"$connector/$name"

  def watermarkStrategy: String

  def properties: Properties
}

object SourceConfig {
  def apply(name: String, config: FlinkConfig): SourceConfig = {
    val p                 = s"sources.$name"
    val watermarkStrategy = Try(config.getString(s"$p.watermark.strategy"))
      .map(config.getWatermarkStrategy)
      .getOrElse(config.watermarkStrategy)

    FlinkConnectorName.withNameInsensitiveOption(
      config.getString(s"$p.connector")
    ) match {
      case Some(connector) =>
        connector match {
          case Kafka      =>
            KafkaSourceConfig(
              connector,
              name,
              config.getString(s"$p.topic"),
              config.getBoolean(s"$p.isKeyed"),
              watermarkStrategy,
              config.getProperties(s"$p.config")
            )
          case Kinesis    =>
            KinesisSourceConfig(
              connector,
              name,
              config.getString(s"$p.stream"),
              watermarkStrategy,
              config.getProperties(s"$p.config")
            )
          case File       =>
            FileSourceConfig(
              connector,
              name,
              config.getString(s"$p.path"),
              watermarkStrategy,
              config.getProperties(s"$p.config")
            )
          case Socket     =>
            SocketSourceConfig(
              connector,
              name,
              config.getString(s"$p.host"),
              config.getInt(s"$p.port"),
              watermarkStrategy,
              config.getProperties(s"$p.config")
            )
          case Collection =>
            CollectionSourceConfig(
              connector,
              name,
              name,
              watermarkStrategy,
              config.getProperties(s"$p.config")
            )
          case other      =>
            throw new RuntimeException(
              s"$other $name connector not valid source (job ${config.jobName}"
            )
        }
      case None            =>
        throw new RuntimeException(
          s"Invalid/missing source connector type for $name (job ${config.jobName}"
        )
    }
  }
}

final case class KafkaSourceConfig(
    connector: FlinkConnectorName = Kafka,
    name: String,
    topic: String,
    isKeyed: Boolean,
    watermarkStrategy: String,
    properties: Properties)
    extends SourceConfig

final case class KinesisSourceConfig(
    connector: FlinkConnectorName = Kinesis,
    name: String,
    stream: String,
    watermarkStrategy: String,
    properties: Properties)
    extends SourceConfig

final case class FileSourceConfig(
    connector: FlinkConnectorName = File,
    name: String,
    path: String,
    watermarkStrategy: String,
    properties: Properties)
    extends SourceConfig

final case class SocketSourceConfig(
    connector: FlinkConnectorName = Socket,
    name: String,
    host: String,
    port: Int,
    watermarkStrategy: String,
    properties: Properties)
    extends SourceConfig

final case class CollectionSourceConfig(
    connector: FlinkConnectorName = Collection,
    name: String,
    topic: String,
    watermarkStrategy: String,
    properties: Properties)
    extends SourceConfig
