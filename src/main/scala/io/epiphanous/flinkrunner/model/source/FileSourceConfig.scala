package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.serde.{
  DelimitedConfig,
  DelimitedRowDecoder,
  JsonRowDecoder,
  RowDecoder
}
import io.epiphanous.flinkrunner.util.FileUtils.getResourceOrFile
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.{Source, SourceSplit}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.connector.file.src.reader.{
  StreamFormat,
  TextLineInputFormat
}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{
  DataStream,
  StreamExecutionEnvironment
}
import org.apache.flink.util.Collector

import java.time.Duration
import scala.util.{Failure, Success}

case class FileSourceConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig,
    connector: FlinkConnectorName = FlinkConnectorName.File)
    extends SourceConfig[ADT] {

  val path: String             = config.getString(pfx("path"))
  val format: StreamFormatName = StreamFormatName.withNameInsensitive(
    config.getStringOpt(pfx("format")).getOrElse("json")
  )
  val isAvroFormat: Boolean    =
    !Seq(StreamFormatName.Parquet, StreamFormatName.Avro)
      .contains(format)
  val badFormatAvroMessage     =
    s"Invalid format ${format.entryName} for avro file source $name. Use ${StreamFormatName.Avro} for plain avro files or ${StreamFormatName.Parquet} for avro parquet files."
  val badFormatNonAvroMessage  =
    s"Invalid format ${format.entryName} for non-avro file source $name."

  val origin: Path          = new Path(getResourceOrFile(path))
  val monitorDuration: Long = properties
    .getProperty("monitor.continuously", "0")
    .toLong

  def getStreamFormat[E <: ADT: TypeInformation]: StreamFormat[E] = ???

  def getAvroStreamFormat[A <: GenericRecord]: StreamFormat[A] =
    ???

  def getRowDecoder[E <: ADT: TypeInformation]: RowDecoder[E] =
    format match {
      case StreamFormatName.Json => new JsonRowDecoder[E]
      case _                     =>
        new DelimitedRowDecoder[E](DelimitedConfig.get(format, properties))
    }

  override def getSource[E <: ADT: TypeInformation]
      : Either[SourceFunction[E], Source[E, _ <: SourceSplit, _]] = {
    require(
      !isAvroFormat,
      badFormatNonAvroMessage
    )
    val fsb =
      FileSource.forRecordStreamFormat(getStreamFormat, origin)
    Right(
      (if (monitorDuration > 0)
         fsb.monitorContinuously(Duration.ofSeconds(monitorDuration))
       else fsb).build()
    )
  }

  /** Creates a source stream for text files (delimited or json). It does
    * this by first using a simple TextLineInputFormat() and then applies a
    * jackson decoder to each line of text to produce the event stream.
    * @param env
    *   stream execution environment
    * @tparam E
    *   type of stream event
    * @return
    *   DataStream[E]
    */
  override def getSourceStream[E <: ADT: TypeInformation](
      env: StreamExecutionEnvironment): DataStream[E] = {
    require(
      !isAvroFormat,
      badFormatNonAvroMessage
    )
    val rawName = s"raw:$label"
    val decoder = getRowDecoder[E]
    val fsb     =
      FileSource
        .forRecordStreamFormat[String](
          new TextLineInputFormat(),
          origin
        )
    if (monitorDuration > 0) {
      fsb.monitorContinuously(
        Duration.ofSeconds(monitorDuration)
      )
    }
    env
      .fromSource(
        fsb.build(),
        WatermarkStrategy.noWatermarks(),
        rawName
      )
      .uid(rawName)
      .flatMap[E](new FlatMapFunction[String, E] {
        override def flatMap(line: String, out: Collector[E]): Unit = {
          decoder.decode(line) match {
            case Failure(error) =>
              logger.warn(
                s"failed to decode $line: ${error.getClass.getName}: ${error.getMessage}",
                error
              )
            case Success(event) =>
              logger.debug(s"decoded [$line] to [$event] successfully")
              out.collect(event)
          }
        }
      })
      .name(label)
      .uid(label)
      .assignTimestampsAndWatermarks(
        getWatermarkStrategy[E]
      )
      .name(s"wm:$label")
      .uid(s"wm:$label")
  }

  /** Create a FileSource for avro parquet files.
    * @param fromKV
    *   a method, available in implicit scope, that creates an instance of
    *   type E from an avro record of type A
    * @tparam E
    *   a type that is a member of the ADT and embeds an avro record of
    *   type A
    * @tparam A
    *   an avro record (should be an instance of SpecificAvroRecord,
    *   although the type here is looser)
    * @return
    */
  override def getAvroSource[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](implicit
      fromKV: EmbeddedAvroRecordInfo[A] => E)
      : Either[SourceFunction[E], Source[E, _ <: SourceSplit, _]] = {
    require(
      format == StreamFormatName.Parquet,
      badFormatAvroMessage
    )
    val fsb =
      FileSource.forRecordStreamFormat(
        new EmbeddedAvroParquetRecordFormat[E, A, ADT](
          getAvroStreamFormat
        ),
        origin
      )
    Right(
      (if (monitorDuration > 0)
         fsb.monitorContinuously(Duration.ofSeconds(monitorDuration))
       else fsb).build()
    )
  }

  override def getAvroSourceStream[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      env: StreamExecutionEnvironment)(implicit
      fromKV: EmbeddedAvroRecordInfo[A] => E): DataStream[E] = {
    format match {
      case StreamFormatName.Parquet => super.getAvroSourceStream[E, A](env)
      case StreamFormatName.Avro    =>
        env.createInput(new EmbeddedAvroInputFormat[E, A, ADT](origin))
      case _                        =>
        throw new RuntimeException(badFormatAvroMessage)
    }
  }
}
