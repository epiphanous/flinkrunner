package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkConnectorName,
  FlinkEvent,
  StreamFormatName
}
import io.epiphanous.flinkrunner.serde.{
  DelimitedConfig,
  DelimitedRowDecoder,
  JsonRowDecoder,
  RowDecoder
}
import io.epiphanous.flinkrunner.util.FileUtils.getResourceOrFile
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.connector.file.src.reader.{
  StreamFormat,
  TextLineInputFormat
}
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.csv.CsvReaderFormat
import org.apache.flink.formats.parquet.avro.AvroParquetReaders
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.{
  CsvMapper,
  CsvSchema
}
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
  val destination: Path        = new Path(getResourceOrFile(path))
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
      case StreamFormatName.Parquet   => getParquetStreamFormat
      case _                          =>
        throw new RuntimeException(
          s"oops, ${format.entryName} not handled by getStreamFileSource"
        )
    }

  def getStreamFileSource[E <: ADT: TypeInformation]: FileSource[E] = {
    val fsb =
      FileSource.forRecordStreamFormat(getStreamFormat, destination)
    (if (monitorDuration > 0)
       fsb.monitorContinuously(Duration.ofSeconds(monitorDuration))
     else fsb).build()
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

  def getParquetStreamFormat[E <: ADT: TypeInformation]: StreamFormat[E] =
    AvroParquetReaders.forReflectRecord(
      implicitly[TypeInformation[E]].getTypeClass
    )

  def getRowDecoder[E <: ADT: TypeInformation]: RowDecoder[E] =
    format match {
      case StreamFormatName.Json => new JsonRowDecoder[E]
      case _                     =>
        new DelimitedRowDecoder[E](DelimitedConfig.get(format, properties))
    }

  def getSource[E <: ADT: TypeInformation](
      env: StreamExecutionEnvironment): DataStream[E] = {
    // all this grossness, because flink hasn't built a JsonStreamFormat?
    (if (format == StreamFormatName.Json) {
       val rawName = s"raw:$label"
       val decoder = getRowDecoder
       val fsb     =
         FileSource
           .forRecordStreamFormat[String](
             new TextLineInputFormat(),
             destination
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
     } else {
       // for all other source streams
       env.fromSource(
         getStreamFileSource[E],
         WatermarkStrategy.noWatermarks(),
         label
       )
     })
      .uid(label)
      .assignTimestampsAndWatermarks(
        getWatermarkStrategy[E]
      )
      .name(s"wm:$label")
      .uid(s"wm:$label")
  }

}
