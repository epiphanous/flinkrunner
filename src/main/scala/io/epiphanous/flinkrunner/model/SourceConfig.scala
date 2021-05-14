package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.model.FlinkConnectorName._
import org.apache.flink.streaming.api.TimeCharacteristic

import java.util.Properties
import scala.util.Try

sealed trait SourceConfig {
  def connector: FlinkConnectorName

  def name: String

  def label: String = s"$connector/$name"

  def timeCharacteristic: TimeCharacteristic

  def watermarkStrategy: String

  def properties: Properties
}

object SourceConfig {
  def apply(name: String, config: FlinkConfig): SourceConfig = {
    val p                  = s"sources.$name"
    val timeCharacteristic =
      Try(config.getString(s"$p.time.characteristic"))
        .map(config.getTimeCharacteristic)
        .getOrElse(config.timeCharacteristic)
    val watermarkStrategy  = Try(config.getString(s"$p.watermark.strategy"))
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
              timeCharacteristic,
              watermarkStrategy,
              config.getProperties(s"$p.config")
            )
          case Kinesis    =>
            KinesisSourceConfig(
              connector,
              name,
              config.getString(s"$p.stream"),
              timeCharacteristic,
              watermarkStrategy,
              config.getProperties(s"$p.config")
            )
          case File       =>
            FileSourceConfig(
              connector,
              name,
              config.getString(s"$p.path"),
              timeCharacteristic,
              watermarkStrategy,
              config.getProperties(s"$p.config")
            )
          case Socket     =>
            SocketSourceConfig(
              connector,
              name,
              config.getString(s"$p.host"),
              config.getInt(s"$p.port"),
              timeCharacteristic,
              watermarkStrategy,
              config.getProperties(s"$p.config")
            )
          case Collection =>
            CollectionSourceConfig(
              connector,
              name,
              name,
              timeCharacteristic,
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
    timeCharacteristic: TimeCharacteristic,
    watermarkStrategy: String,
    properties: Properties)
    extends SourceConfig

final case class KinesisSourceConfig(
    connector: FlinkConnectorName = Kinesis,
    name: String,
    stream: String,
    timeCharacteristic: TimeCharacteristic,
    watermarkStrategy: String,
    properties: Properties)
    extends SourceConfig

final case class FileSourceConfig(
    connector: FlinkConnectorName = File,
    name: String,
    path: String,
    timeCharacteristic: TimeCharacteristic,
    watermarkStrategy: String,
    properties: Properties)
    extends SourceConfig

final case class SocketSourceConfig(
    connector: FlinkConnectorName = Socket,
    name: String,
    host: String,
    port: Int,
    timeCharacteristic: TimeCharacteristic,
    watermarkStrategy: String,
    properties: Properties)
    extends SourceConfig

final case class CollectionSourceConfig(
    connector: FlinkConnectorName = Collection,
    name: String,
    topic: String,
    timeCharacteristic: TimeCharacteristic,
    watermarkStrategy: String,
    properties: Properties)
    extends SourceConfig
