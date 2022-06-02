package io.epiphanous.flinkrunner.model.source

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkConnectorName._
import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkConnectorName,
  RabbitMQConnectionInfo,
  StreamFormatName
}
import io.epiphanous.flinkrunner.util.ConfigToProps
import io.epiphanous.flinkrunner.util.StreamUtils._
import org.apache.flink.connector.kafka.source.enumerator.initializer.{
  NoStoppingOffsetsInitializer,
  OffsetsInitializer
}
import org.apache.kafka.clients.consumer.OffsetResetStrategy

import java.util
import java.util.Properties
import scala.concurrent.duration.DurationInt
import scala.util.Try

trait SourceConfig extends LazyLogging {
  def config: FlinkConfig

  def connector: FlinkConnectorName

  def name: String

  def label: String = s"$connector/$name"

  def watermarkStrategy: String

  def maxAllowedLateness: Long

  def properties: Properties

  def propertiesMap: util.HashMap[String, String] =
    properties.asJavaMap
}

object SourceConfig {
  def apply(name: String, config: FlinkConfig): SourceConfig = {
    val p                  = s"sources.$name"
    val maxAllowedLateness = Try(
      config.getDuration(s"$p.max.allowed.lateness")
    ).map(_.toMillis).getOrElse(5.minutes.toMillis)
    val watermarkStrategy  = Try(config.getString(s"$p.watermark.strategy"))
      .map(config.getWatermarkStrategy)
      .getOrElse(config.watermarkStrategy)
    val connector          = FlinkConnectorName
      .fromSourceName(
        name,
        config.jobName,
        config.getStringOpt(s"$p.connector"),
        Some(Collection)
      )

    connector match {
      case Kafka      =>
        val props = ConfigToProps.normalizeProps(
          config,
          p,
          List("bootstrap.servers")
        )
        KafkaSourceConfig(
          config,
          connector,
          name,
          config.getString(s"$p.topic"),
          config.getBoolean(s"$p.isKeyed"),
          watermarkStrategy,
          props.getProperty("bootstrap.servers"),
          maxAllowedLateness,
          config.getBooleanOpt(s"$p.bounded").getOrElse(false),
          config.getStringOpt(s"$p.starting.offset") match {
            case Some(o) if o.equalsIgnoreCase("earliest") =>
              OffsetsInitializer.earliest()
            case Some(o) if o.equalsIgnoreCase("latest")   =>
              OffsetsInitializer.latest()
            case Some(o) if o.matches("[0-9]+")            =>
              OffsetsInitializer.timestamp(o.toLong)
            case _                                         =>
              OffsetsInitializer.committedOffsets(
                OffsetResetStrategy.EARLIEST
              )
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
          props
        )
      case Kinesis    =>
        KinesisSourceConfig(
          config,
          connector,
          name,
          config.getString(s"$p.stream"),
          watermarkStrategy,
          maxAllowedLateness,
          config.getProperties(s"$p.config")
        )
      case File       =>
        val format =
          config.getStringOpt(s"$p.format").getOrElse("json")
        FileSourceConfig(
          config,
          connector,
          name,
          config.getString(s"$p.path"),
          StreamFormatName.withNameInsensitive(format),
          watermarkStrategy,
          maxAllowedLateness,
          config.getProperties(s"$p.config")
        )
      case Socket     =>
        val format = config.getStringOpt("$p.format").getOrElse("csv")
        SocketSourceConfig(
          config,
          connector,
          name,
          config.getString(s"$p.host"),
          config.getInt(s"$p.port"),
          StreamFormatName.withNameInsensitive(format),
          watermarkStrategy,
          maxAllowedLateness,
          config.getProperties(s"$p.config")
        )
      case Collection =>
        CollectionSourceConfig(
          config,
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
          config,
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
      case _          =>
        throw new RuntimeException(
          s"Don't know how to configure ${connector.entryName} source connector $name in job ${config.jobName}"
        )
    }
  }
}
