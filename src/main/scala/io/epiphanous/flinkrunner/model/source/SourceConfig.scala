package io.epiphanous.flinkrunner.model.source

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkConnectorName._
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.util.BoundedLatenessWatermarkStrategy
import io.epiphanous.flinkrunner.util.StreamUtils._
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.{Source, SourceSplit}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import java.time.Duration
import java.util
import java.util.Properties
import scala.util.Try

trait SourceConfig[ADT <: FlinkEvent] extends LazyLogging {
  def name: String
  def config: FlinkConfig
  def connector: FlinkConnectorName

  def pfx(path: String = ""): String = Seq(
    Some("sources"),
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

  def getSource[E <: ADT: TypeInformation]
      : Either[SourceFunction[E], Source[E, _ <: SourceSplit, _]] =
    ??? // intentionally unimplemented

  def getSourceStream[E <: ADT: TypeInformation](
      env: StreamExecutionEnvironment): DataStream[E] = {
    getSource
      .fold(
        f =>
          env
            .addSource(f)
            .assignTimestampsAndWatermarks(getWatermarkStrategy)
            .name(label),
        s => env.fromSource(s, getWatermarkStrategy, label)
      )
      .uid(label)
  }

  def getAvroSource[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](implicit
      fromKV: EmbeddedAvroRecordInfo[A] => E)
      : Either[SourceFunction[E], Source[E, _ <: SourceSplit, _]] =
    ??? // intentionally unimplemented

  def getAvroSourceStream[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      env: StreamExecutionEnvironment)(implicit
      fromKV: EmbeddedAvroRecordInfo[A] => E): DataStream[E] =
    getAvroSource[E, A]
      .fold(
        f =>
          env
            .addSource(f)
            .assignTimestampsAndWatermarks(getWatermarkStrategy)
            .name(label),
        s => env.fromSource(s, getWatermarkStrategy, label)
      )
      .uid(label)
}

object SourceConfig {
  def apply[ADT <: FlinkEvent](
      name: String,
      config: FlinkConfig): SourceConfig[ADT] = {
    FlinkConnectorName
      .fromSourceName(
        name,
        config.jobName,
        config.getStringOpt(s"sources.$name.connector")
      ) match {
      case File       => FileSourceConfig(name, config, File)
      case Hybrid     => HybridSourceConfig(name, config, Hybrid)
      case Kafka      => KafkaSourceConfig[ADT](name, config, Kafka)
      case Kinesis    => KinesisSourceConfig(name, config, Kinesis)
      case RabbitMQ   => RabbitMQSourceConfig(name, config, RabbitMQ)
      case Socket     => SocketSourceConfig(name, config, Socket)
      case MockSource => MockSourceConfig(name, config, MockSource)
      case connector  =>
        throw new RuntimeException(
          s"Don't know how to configure ${connector.entryName} source connector $name in job ${config.jobName}"
        )
    }
  }
}
