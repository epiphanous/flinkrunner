package io.epiphanous.flinkrunner.model.source

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model.FlinkConnectorName._
import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkConnectorName,
  FlinkEvent
}
import io.epiphanous.flinkrunner.util.BoundedLatenessWatermarkStrategy
import io.epiphanous.flinkrunner.util.StreamUtils._
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.time.Duration
import java.util
import java.util.Properties
import scala.util.Try

trait SourceConfig[ADT <: FlinkEvent] extends LazyLogging {
  def name: String
  def config: FlinkConfig
  def connector: FlinkConnectorName

  def pfx(path: String = ""): String = Seq(
    Some("sinks"),
    Some(name),
    if (path.isEmpty) None else Some(path)
  ).flatten.mkString(".")

  val properties: Properties = config.getProperties(pfx("config"))

  lazy val propertiesMap: util.HashMap[String, String] =
    properties.asJavaMap

  val label: String = s"${connector.entryName.toLowerCase}/$name"

  val watermarkStrategy: String =
    Try(config.getString(pfx("watermark.strategy")))
      .map(config.getWatermarkStrategy)
      .getOrElse(config.watermarkStrategy)

  val maxAllowedLateness: Option[Duration] = Seq(
    Try(
      config.getDuration(pfx("max.allowed.lateness"))
    ).toOption,
    config.maxLateness
  ).flatten.headOption

  val maxIdleness: Option[Duration] = Seq(
    Try(
      config.getDuration(pfx("max.allowed.lateness"))
    ).toOption,
    config.maxIdleness
  ).flatten.headOption

  def getWatermarkStrategy[E <: ADT: TypeInformation]
      : WatermarkStrategy[E] = {
    val ws = watermarkStrategy match {
      case "none"                 => WatermarkStrategy.noWatermarks[E]()
      case "bounded out of order" =>
        WatermarkStrategy
          .forBoundedOutOfOrderness[E](
            maxAllowedLateness.getOrElse(
              throw new RuntimeException(
                s"source $name uses a bounded out of order watermark strategy but has no max.lateness duration configured"
              )
            )
          )
      case "ascending timestamps" =>
        WatermarkStrategy.forMonotonousTimestamps[E]()
      case _                      =>
        new BoundedLatenessWatermarkStrategy[E](
          this.maxAllowedLateness
            .getOrElse(
              throw new RuntimeException(
                s"source $name uses a bounded lateness watermark strategy but has no max.lateness duration configured"
              )
            )
            .toMillis,
          name
        )
    }
    maxIdleness.map(idleness => ws.withIdleness(idleness)).getOrElse(ws)
  }
}

object SourceConfig {
  def apply[ADT <: FlinkEvent](
      name: String,
      runner: FlinkRunner[ADT]): SourceConfig[ADT] = {
    val config = runner.config
    FlinkConnectorName
      .fromSourceName(
        name,
        config.jobName,
        config.getStringOpt(s"sources.$name.connector"),
        Some(Collection)
      ) match {
      case Kafka      => KafkaSourceConfig[ADT](name, config, Kafka)
      case Kinesis    => KinesisSourceConfig(name, config, Kinesis)
      case File       => FileSourceConfig(name, config, File)
      case Socket     => SocketSourceConfig(name, config, Socket)
      case Collection =>
        CollectionSourceConfig[ADT](name, config, Collection)
      case RabbitMQ   => RabbitMQSourceConfig(name, config, RabbitMQ)
      case connector  =>
        throw new RuntimeException(
          s"Don't know how to configure ${connector.entryName} source connector $name in job ${config.jobName}"
        )
    }
  }
}
