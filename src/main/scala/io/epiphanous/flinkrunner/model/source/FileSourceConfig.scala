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
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.{Source, SourceSplit}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.connector.file.src.reader.{
  StreamFormat,
  TextLineInputFormat
}
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.csv.CsvReaderFormat
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.{
  CsvMapper,
  CsvSchema
}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{
  DataStream,
  StreamExecutionEnvironment
}

import java.time.Duration

case class FileSourceConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig,
    connector: FlinkConnectorName = FlinkConnectorName.File)
    extends SourceConfig[ADT] {

  val path: String             = config.getString(pfx("path"))
  val format: StreamFormatName = StreamFormatName.withNameInsensitive(
    config.getStringOpt(pfx("format")).getOrElse("json")
  )
  val origin: Path             = new Path(getResourceOrFile(path))
  val monitorDuration: Long    = properties
    .getProperty("monitor.continuously", "0")
    .toLong

  def getStreamFormat[E <: ADT: TypeInformation]: StreamFormat[E] =
    format match {
      case StreamFormatName.Csv       => getCsvStreamFormat
      case StreamFormatName.Tsv       => getTsvStreamFormat
      case StreamFormatName.Psv       => getPsvStreamFormat
      case StreamFormatName.Delimited =>
        getDelimitedStreamFormat(DelimitedConfig.get(format, properties))
      case _                          =>
        throw new RuntimeException(
          s"oops, ${format.entryName} files not handled by getStreamFormat for FileSource"
        )
    }

  def getCsvStreamFormat[E <: ADT: TypeInformation]: StreamFormat[E] =
    getDelimitedStreamFormat(DelimitedConfig.CSV)
  def getTsvStreamFormat[E <: ADT: TypeInformation]: StreamFormat[E] =
    getDelimitedStreamFormat(DelimitedConfig.TSV)
  def getPsvStreamFormat[E <: ADT: TypeInformation]: StreamFormat[E] =
    getDelimitedStreamFormat(DelimitedConfig.PSV)
  def getDelimitedStreamFormat[E <: ADT: TypeInformation](
      c: DelimitedConfig): StreamFormat[E] = {
    val ti                = implicitly[TypeInformation[E]]
    val mapper            = new CsvMapper()
    val schema: CsvSchema = mapper
      .schemaFor(ti.getTypeClass)
      .withColumnSeparator(c.columnSeparator)
      .withQuoteChar(c.quoteCharacter)
      .withEscapeChar(c.escapeChar)
      .withLineSeparator(c.lineSeparator)
    CsvReaderFormat
      .forSchema[E](mapper, schema, ti)
  }

  def getRowDecoder[E <: ADT: TypeInformation]: RowDecoder[E] =
    format match {
      case StreamFormatName.Json => new JsonRowDecoder[E]
      case _                     =>
        new DelimitedRowDecoder[E](DelimitedConfig.get(format, properties))
    }

  override def getSource[E <: ADT: TypeInformation]
      : Either[SourceFunction[E], Source[E, _ <: SourceSplit, _]] = {
    val fsb =
      FileSource.forRecordStreamFormat(getStreamFormat, origin)
    Right(
      (if (monitorDuration > 0)
         fsb.monitorContinuously(Duration.ofSeconds(monitorDuration))
       else fsb).build()
    )
  }

  override def getSourceStream[E <: ADT: TypeInformation](
      env: StreamExecutionEnvironment): DataStream[E] = {
    // all this grossness, because flink hasn't built a JsonStreamFormat?
    if (format == StreamFormatName.Json) {
      val rawName = s"raw:$label"
      val decoder = getRowDecoder
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
        .flatMap(line => decoder.decode(line).toOption)
        .name(label)
        .uid(label)
        .assignTimestampsAndWatermarks(
          getWatermarkStrategy[E]
        )
        .name(s"wm:$label")
        .uid(s"wm:$label")
    } else
      super.getSourceStream(env)

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
      fromKV: EmbeddedAvroRecordInfo[A] => E,
      avroParquetRecordFormat: StreamFormat[A])
      : Either[SourceFunction[E], Source[E, _ <: SourceSplit, _]] = {
    val fsb =
      FileSource.forRecordStreamFormat(
        new EmbeddedAvroParquetRecordFormat[E, A, ADT],
        origin
      )
    Right(
      (if (monitorDuration > 0)
         fsb.monitorContinuously(Duration.ofSeconds(monitorDuration))
       else fsb).build()
    )
  }
}
