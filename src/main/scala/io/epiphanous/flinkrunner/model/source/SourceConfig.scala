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

/** A flinkrunner source configuration trait. All source configs have a few
  * common configuration options.
  *
  * Common Configuration Options:
  *
  *   - `name`: the source name
  *   - `connector`: one of
  *     - [[FlinkConnectorName.File]]
  *     - [[FlinkConnectorName.Hybrid]]
  *     - [[FlinkConnectorName.Kafka]]
  *     - [[FlinkConnectorName.Kinesis]]
  *     - [[FlinkConnectorName.RabbitMQ]]
  *     - [[FlinkConnectorName.Socket]]
  *   - `watermark.strategy`: one of:
  *     - `bounded out of orderness`
  *     - `bounded lateness`
  *     - `ascending timestamps`
  *     - `none`: don't use watermarks with this source
  *   - `max.allowed.lateness`: the lateness allowed with bounded lateness
  *     watermarks
  *   - `max.idleness`: the maximum duration to wait for new events before
  *     advancing a watermark when using bounded lateness watermarks
  *   - `config`: properties to pass to the underlying flink connector
  * @tparam ADT
  *   Flinkrunner algebraic data type
  */
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

  lazy val label: String = s"${connector.entryName.toLowerCase}/$name"

  val watermarkStrategy: String =
    Try(config.getString(pfx("watermark.strategy")))
      .map(config.getWatermarkStrategy)
      .getOrElse(config.watermarkStrategy)

  val maxAllowedLateness: Option[Duration] = Seq(
    config.getDurationOpt(pfx("max.allowed.lateness")),
    config.maxLateness
  ).flatten.headOption

  val maxIdleness: Option[Duration] = Seq(
    config.getDurationOpt(pfx("max.idleness")),
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

  /** Return either a SourceFunction[E] or a Source[E] where E is a flink
    * event type. No implementation is provided here. Subclasses must
    * either override this method and use the provided [[getSourceStream]]
    * method, or override the [[getAvroSourceStream]] implementation.
    *
    * @tparam E
    *   the flink event type (subclass of ADT)
    * @return
    *   Either[ SourceFunction[E], Source[E] ]
    */
  def getSource[E <: ADT: TypeInformation]
      : Either[SourceFunction[E], Source[E, _ <: SourceSplit, _]] =
    ??? // intentionally unimplemented

  /** Create a source DataStream[E] of events. This default implementation
    * relies on the [[getSource]] method to return either an (older style)
    * flink source function or a (newer style) unified source api instance,
    * from which it creates the data stream using the provided flink stream
    * execution environment.
    *
    * @param env
    *   flink stream execution environment
    * @tparam E
    *   event type (subclass of ADT)
    * @return
    */
  def getSourceStreamDefault[E <: ADT: TypeInformation](
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

  /** Flinkrunner calls this method to create a source stream from
    * configuration. This uses the default implementation provided in
    * [[getSourceStreamDefault()]]. Subclasses can override this to provide
    * other implementations.
    *
    * @param env
    *   a flink stream execution environment
    * @tparam E
    *   the event stream type
    * @return
    *   DataStream[E]
    */
  def getSourceStream[E <: ADT: TypeInformation](
      env: StreamExecutionEnvironment): DataStream[E] =
    getSourceStreamDefault[E](env)

  /** Return either a SourceFunction[E] or a Source[E] where E is an event
    * that embeds an avro record of type A. No implementation is provided
    * here. Subclasses must either override this method and use the
    * provided [[getAvroSourceStream]] method, or override the
    * [[getAvroSourceStream]] implementation.
    *
    * @param fromKV
    *   an implicitly provided method to create an event of type E from an
    *   avro record of type A
    * @tparam E
    *   an ADT event that embeds an avro record of type A
    * @tparam A
    *   an avro record type (specific or generic)
    * @return
    *   Either[ SourceFunction[E], Source[E] ]
    */
  def getAvroSource[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](implicit
      fromKV: EmbeddedAvroRecordInfo[A] => E)
      : Either[SourceFunction[E], Source[E, _ <: SourceSplit, _]] =
    ??? // intentionally unimplemented

  /** Create a source DataStream[E] of events that embed an avro record of
    * type A. This default implementation relies on the [[getAvroSource]]
    * method to return either an (older style) flink source function or a
    * (newer style) unified source api instance, from which it creates the
    * data stream using the provided flink stream execution environment.
    *
    * @param env
    *   a flink stream execution environment
    * @param fromKV
    *   implicitly provided function that creates an event of type E from
    *   an avro event of type A
    * @tparam E
    *   an ADT event type that embeds an avro event of type A
    * @tparam A
    *   an avro record type (generic or specific)
    * @return
    *   DataStream[E]
    */
  def getAvroSourceStreamDefault[
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

  /** Flinkrunner calls this method to create an avro source stream. This
    * method uses the default implementation in
    * [[getAvroSourceStreamDefault()]]. Subclasses can provide their own
    * implentations.
    * @param env
    *   a flink stream execution environment
    * @param fromKV
    *   an implicit method to create events of type E from avro records
    * @tparam E
    *   the stream event type, which embeds an avro type A
    * @tparam A
    *   an avro record type, embedded in E
    * @return
    *   DataStream[E]
    */
  def getAvroSourceStream[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      env: StreamExecutionEnvironment)(implicit
      fromKV: EmbeddedAvroRecordInfo[A] => E): DataStream[E] =
    getAvroSourceStreamDefault[E, A](env)
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
      case File      => FileSourceConfig[ADT](name, config)
      case Hybrid    => HybridSourceConfig[ADT](name, config)
      case Kafka     => KafkaSourceConfig[ADT](name, config)
      case Kinesis   => KinesisSourceConfig[ADT](name, config)
      case RabbitMQ  => RabbitMQSourceConfig[ADT](name, config)
      case Socket    => SocketSourceConfig[ADT](name, config)
      case Generator =>
        throw new RuntimeException(
          s"You must provide a concrete subclass of GeneratorSourceConfig for source connector $name in job ${config.jobName}"
        )
      case connector =>
        throw new RuntimeException(
          s"Don't know how to configure ${connector.entryName} source connector $name in job ${config.jobName}"
        )
    }
  }
}
