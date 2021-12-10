package io.epiphanous.flinkrunner.model

import com.google.common.collect.Maps
import io.epiphanous.flinkrunner.model.FlinkConnectorName._
import org.apache.flink.connector.kafka.source.enumerator.initializer.{
  NoStoppingOffsetsInitializer,
  OffsetsInitializer
}

import java.util
import java.util.Properties
import scala.concurrent.duration.DurationInt
import scala.util.Try

sealed trait SourceConfig {
  def connector: FlinkConnectorName

  def name: String

  def label: String = s"$connector/$name"

  def watermarkStrategy: String

  def maxAllowedLateness: Long

  def properties: Properties

  def propertiesMap: util.HashMap[String, String] =
    Maps.newHashMap(Maps.fromProperties(properties))
}

object SourceConfig {
  def apply[ADT <: FlinkEvent](
      name: String,
      config: FlinkConfig[ADT]): SourceConfig = {
    val p                  = s"sources.$name"
    val maxAllowedLateness = Try(
      config.getDuration(s"$p.max.allowed.lateness")
    ).map(_.toMillis).getOrElse(5.minutes.toMillis)
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
              watermarkStrategy,
              maxAllowedLateness,
              config.getBooleanOpt(s"$p.bounded").getOrElse(false),
              config.getStringOpt(s"$p.starting.offset") match {
                case Some(o) if o.equalsIgnoreCase("earliest")  =>
                  OffsetsInitializer.earliest()
                case Some(o) if o.equalsIgnoreCase("latest")    =>
                  OffsetsInitializer.latest()
                case Some(o) if o.equalsIgnoreCase("committed") =>
                  OffsetsInitializer.committedOffsets()
                case Some(o) if o.matches("[0-9]+")             =>
                  OffsetsInitializer.timestamp(o.toLong)
                case _                                          => OffsetsInitializer.latest()
              },
              config.getStringOpt(s"$p.stopping.offset") match {
                case Some(o) if o.equalsIgnoreCase("latest")    =>
                  OffsetsInitializer.latest()
                case Some(o) if o.equalsIgnoreCase("committed") =>
                  OffsetsInitializer.committedOffsets()
                case Some(o) if o.matches("[0-9]+")             =>
                  OffsetsInitializer.timestamp(o.toLong)
                case _                                          => new NoStoppingOffsetsInitializer()
              },
              config.getProperties(s"$p.config")
            )
          case Kinesis    =>
            KinesisSourceConfig(
              connector,
              name,
              config.getString(s"$p.stream"),
              watermarkStrategy,
              maxAllowedLateness,
              config.getProperties(s"$p.config")
            )
          case File       =>
            FileSourceConfig(
              connector,
              name,
              config.getString(s"$p.path"),
              watermarkStrategy,
              maxAllowedLateness,
              config.getProperties(s"$p.config")
            )
          case Socket     =>
            SocketSourceConfig(
              connector,
              name,
              config.getString(s"$p.host"),
              config.getInt(s"$p.port"),
              watermarkStrategy,
              maxAllowedLateness,
              config.getProperties(s"$p.config")
            )
          case Collection =>
            CollectionSourceConfig(
              connector,
              name,
              name,
              watermarkStrategy,
              maxAllowedLateness,
              config.getProperties(s"$p.config")
            )
          case RabbitMQ   =>
            val c   = config.getProperties(s"$p.config")
            val uri = config.getString(s"$p.uri")
            RabbitMQSourceConfig(
              connector,
              name,
              uri,
              config.getBoolean(s"$p.use.correlation.id"),
              config.getString(s"$p.queue"),
              watermarkStrategy,
              maxAllowedLateness,
              RabbitMQConnectionInfo(uri, c),
              c
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
    maxAllowedLateness: Long,
    bounded: Boolean = false,
    startingOffsets: OffsetsInitializer,
    stoppingOffsets: OffsetsInitializer,
    properties: Properties)
    extends SourceConfig

final case class KinesisSourceConfig(
    connector: FlinkConnectorName = Kinesis,
    name: String,
    stream: String,
    watermarkStrategy: String,
    maxAllowedLateness: Long,
    properties: Properties)
    extends SourceConfig

final case class FileSourceConfig(
    connector: FlinkConnectorName = File,
    name: String,
    path: String,
    watermarkStrategy: String,
    maxAllowedLateness: Long,
    properties: Properties)
    extends SourceConfig

final case class SocketSourceConfig(
    connector: FlinkConnectorName = Socket,
    name: String,
    host: String,
    port: Int,
    watermarkStrategy: String,
    maxAllowedLateness: Long,
    properties: Properties)
    extends SourceConfig

final case class CollectionSourceConfig(
    connector: FlinkConnectorName = Collection,
    name: String,
    topic: String,
    watermarkStrategy: String,
    maxAllowedLateness: Long,
    properties: Properties)
    extends SourceConfig

final case class RabbitMQSourceConfig(
    connector: FlinkConnectorName = RabbitMQ,
    name: String,
    uri: String,
    useCorrelationId: Boolean,
    queue: String,
    watermarkStrategy: String,
    maxAllowedLateness: Long,
    connectionInfo: RabbitMQConnectionInfo,
    properties: Properties)
    extends SourceConfig
