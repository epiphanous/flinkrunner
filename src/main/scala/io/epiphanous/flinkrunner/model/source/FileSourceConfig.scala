package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.model.FlinkConnectorName.File
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
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.connector.file.src.reader.StreamFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.csv.CsvReaderFormat
import org.apache.flink.formats.parquet.avro.AvroParquetReaders
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.{
  CsvMapper,
  CsvSchema
}

import java.time.Duration
import java.util.Properties

case class FileSourceConfig(
    config: FlinkConfig,
    connector: FlinkConnectorName = File,
    name: String,
    path: String,
    format: StreamFormatName,
    watermarkStrategy: String,
    maxAllowedLateness: Long,
    properties: Properties)
    extends SourceConfig {

  val destination: Path     = new Path(getResourceOrFile(path))
  val monitorDuration: Long = properties
    .getProperty("monitor.continuously", "0")
    .toLong

  def getStreamFileSource[E <: FlinkEvent: TypeInformation]
      : FileSource[E] = {

    val streamFormat = format match {
      case StreamFormatName.Csv       => getCsvStreamFormat
      case StreamFormatName.Tsv       => getTsvStreamFormat
      case StreamFormatName.Psv       => getPsvStreamFormat
      case StreamFormatName.Delimited =>
        getDelimitedStreamFormat(DelimitedConfig.get(format, properties))
      case StreamFormatName.Parquet   => getParquetStreamFormat
      case StreamFormatName.Json      =>
        throw new RuntimeException(
          "oops, json not handled by getStreamFileSource"
        )
    }

    val fsb = FileSource.forRecordStreamFormat(streamFormat, destination)
    (if (monitorDuration > 0)
       fsb.monitorContinuously(Duration.ofSeconds(monitorDuration))
     else fsb).build()
  }

  def getCsvStreamFormat[E <: FlinkEvent: TypeInformation]
      : StreamFormat[E] = getDelimitedStreamFormat(DelimitedConfig.CSV)
  def getTsvStreamFormat[E <: FlinkEvent: TypeInformation]
      : StreamFormat[E] = getDelimitedStreamFormat(DelimitedConfig.TSV)
  def getPsvStreamFormat[E <: FlinkEvent: TypeInformation]
      : StreamFormat[E] = getDelimitedStreamFormat(DelimitedConfig.PSV)
  def getDelimitedStreamFormat[E <: FlinkEvent: TypeInformation](
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

  def getParquetStreamFormat[E <: FlinkEvent: TypeInformation]
      : StreamFormat[E] = AvroParquetReaders.forReflectRecord(
    implicitly[TypeInformation[E]].getTypeClass
  )

  def getRowDecoder[E: TypeInformation]: RowDecoder[E] = format match {
    case StreamFormatName.Json => new JsonRowDecoder[E]
    case _                     =>
      new DelimitedRowDecoder[E](DelimitedConfig.get(format, properties))
  }

}
