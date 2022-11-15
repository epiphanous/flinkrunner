package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.serde._
import io.epiphanous.flinkrunner.util.AvroUtils.toEmbeddedAvroInstance
import io.epiphanous.flinkrunner.util.ConfigToProps.getFromEither
import io.epiphanous.flinkrunner.util.FileUtils.getResourceOrFile
import org.apache.avro.Schema
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
import org.apache.flink.formats.avro.AvroInputFormat
import org.apache.flink.streaming.api.functions.source.{
  FileProcessingMode,
  SourceFunction
}
import org.apache.flink.streaming.api.scala.{
  DataStream,
  StreamExecutionEnvironment
}
import org.apache.flink.util.Collector

import java.time.Duration

/** A source config for reading files as a source for a flink job. For
  * example, the following config can be used to read a (possibly nested)
  * directory of csv data files stored in s3, and monitor it continuously
  * every minute for new files.
  * {{{
  *   source my-file-source {
  *     path = "s3://my-bucket/data"
  *     format = csv
  *     monitor = 1m
  *   }
  * }}}
  *
  * Configuration options:
  *   - `path`: a required config specifying the location of the directory
  *     or file
  *   - `format`: an optional config (defaults to `json`) identifying the
  *     file format, which can be one of
  *     - `json`: line separated json objects
  *     - `csv`: (comma delimited)
  *     - `psv`: pipe separated values
  *     - `tsv`: tab separated values
  *     - `delimited`: general delimited (see config options below)
  *     - `avro`: avro file format
  *     - `parquet`: parquet file format
  *     - `monitor`: an optional duration indicating how frequently the
  *       file location should be monitored for new files. Defaults to 0,
  *       meaning no new files will be monitored.
  *     - `column.separator`: optional string that separate columns
  *       (defaults to a comma if format is delimited)
  *     - `line.separator`: optional string that separate lines (defaults
  *       to the system line separator if format is delimited)
  *     - `quote.character`: optional character that quotes values
  *       (defaults to double quote if format is delimited)
  *     - `escape.character`: optional character that escapes quote
  *       characters in values (defaults to backslash if format is
  *       delimited)
  *     - `uses.header`: true if files contain a header line (defaults to
  *       true)
  *     - `uses.quotes`: true if all column values should be quoted or only
  *       those that have embedded quotes (defaults to false)
  *
  * @param name
  *   name of the source
  * @param config
  *   flinkrunner config
  * @tparam ADT
  *   flinkrunner algebraic data type
  */
case class FileSourceConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig)
    extends SourceConfig[ADT] {

  override val connector: FlinkConnectorName = FlinkConnectorName.File

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

  val origin: Path = new Path(getResourceOrFile(path))

  val monitorDuration: Long = getFromEither(
    pfx(),
    Seq(
      "monitor",
      "monitor.duration",
      "monitor.continuously",
      "monitor.every",
      "monitor.continuously.every"
    ),
    config.getDurationOpt
  ).map(_.toSeconds).getOrElse(0)

  val includePaths: List[String] =
    config.getStringListOpt(pfx("paths.to.include"))
  val excludePaths: List[String] =
    config.getStringListOpt(pfx("paths.to.exclude"))

  val wantsFiltering: Boolean =
    includePaths.nonEmpty || excludePaths.nonEmpty

  val fileFilter: FileSourcePathFilter[ADT]                     = FileSourcePathFilter(this)
  val fileEnumeratorProvider: FileSourceEnumeratorProvider[ADT] =
    FileSourceEnumeratorProvider(
      this
    )

  val delimitedConfig: DelimitedConfig =
    DelimitedConfig.get(format, pfx(), config)

  def getStreamFormat[E <: ADT: TypeInformation]: StreamFormat[E] = ???

  /** Important: If users need to read parquet files and produce events
    * with embedded GenericRecord instances, you must override this and
    * provide a schema.
    */
  def genericAvroSchema: Option[Schema] = None

  def getRowDecoder[E <: ADT: TypeInformation]: RowDecoder[E] = {
    require(
      format.isText,
      s"getRowDecoder can't decode non-text format $format"
    )
    format match {
      case StreamFormatName.Json => new JsonRowDecoder[E]
      case _                     =>
        new DelimitedRowDecoder[E](delimitedConfig)
    }
  }

  override def getSource[E <: ADT: TypeInformation]
      : Either[SourceFunction[E], Source[E, _ <: SourceSplit, _]] = {
    require(
      !isAvroFormat,
      badFormatNonAvroMessage
    )

    val fsb =
      FileSource.forRecordStreamFormat(getStreamFormat, origin)
    if (wantsFiltering) {
      fsb.setFileEnumerator(fileEnumeratorProvider)
    }
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
      env: StreamExecutionEnvironment): DataStream[E] =
    if (format.isText)
      flatMapTextStream(getTextFileStream(env), getRowDecoder[E])
    else super.getSourceStream(env)

  def getTextFileStream(
      env: StreamExecutionEnvironment): DataStream[String] = {
    val rawName = s"raw:$label"
    val fsb     =
      FileSource
        .forRecordStreamFormat[String](
          new TextLineInputFormat(),
          origin
        )
    if (wantsFiltering)
      fsb.setFileEnumerator(fileEnumeratorProvider)
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
  }

  def nameAndWatermark[E <: ADT: TypeInformation](
      stream: DataStream[E],
      label: String): DataStream[E] = {
    stream
      .name(label)
      .uid(label)
      .assignTimestampsAndWatermarks(
        getWatermarkStrategy[E]
      )
      .name(s"wm:$label")
      .uid(s"wm:$label")
  }

  def flatMapTextStream[E <: ADT: TypeInformation](
      textStream: DataStream[String],
      decoder: RowDecoder[E]): DataStream[E] = {
    nameAndWatermark(
      textStream
        .flatMap[E](new FlatMapFunction[String, E] {
          override def flatMap(line: String, out: Collector[E]): Unit =
            decoder.decode(line).foreach { e =>
              println(s"decoded event from $line")
              out.collect(e)
            }
        }),
      label
    )
  }

  def getTextAvroSourceStream[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      env: StreamExecutionEnvironment)(implicit
      fromKV: EmbeddedAvroRecordInfo[A] => E): DataStream[E] = {
    require(
      format.isText,
      s"getTextAvroSourceStream called on non-text format $format"
    )
    val decoder = format match {
      case StreamFormatName.Json  =>
        new EmbeddedAvroJsonRowDecoder[E, A, ADT](config)
      case fmt if fmt.isDelimited =>
        new EmbeddedAvroDelimitedRowDecoder[E, A, ADT](
          config,
          delimitedConfig
        )
      case _                      =>
        throw new RuntimeException(
          s"getTextAvroSourceStream can't handle text format $format"
        )
    }
    flatMapTextStream(getTextFileStream(env), decoder)
  }

  def getBulkAvroSourceStream[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      env: StreamExecutionEnvironment)(implicit
      fromKV: EmbeddedAvroRecordInfo[A] => E): DataStream[E] = {
    require(
      format.isBulk,
      s"getBulkAvroSourceStream called on non-bulk format $format"
    )
    format match {
      case StreamFormatName.Parquet =>
        val fsb =
          FileSource.forRecordStreamFormat(
            new EmbeddedAvroParquetInputFormat[E, A, ADT](
              this,
              genericAvroSchema
            ),
            origin
          )
        if (wantsFiltering)
          fsb.setFileEnumerator(fileEnumeratorProvider)
        env
          .fromSource(
            (if (monitorDuration > 0)
               fsb.monitorContinuously(Duration.ofSeconds(monitorDuration))
             else fsb).build(),
            getWatermarkStrategy,
            label
          )
      case StreamFormatName.Avro    =>
        val avroInputFormat = new AvroInputFormat(
          origin,
          classOf[GenericRecord]
        )
        avroInputFormat.setNestedFileEnumeration(true)
        if (wantsFiltering) avroInputFormat.setFilesFilter(fileFilter)
        nameAndWatermark(
          env
            .readFile[GenericRecord](
              avroInputFormat,
              path,
              if (monitorDuration > 0)
                FileProcessingMode.PROCESS_CONTINUOUSLY
              else FileProcessingMode.PROCESS_ONCE,
              monitorDuration
            )
            .uid(s"avro:$label")
            .name(s"avro:$label")
            .map(g =>
              toEmbeddedAvroInstance[E, A, ADT](
                g,
                implicitly[TypeInformation[A]].getTypeClass,
                config
              )
            ),
          label
        )
      case _                        =>
        throw new RuntimeException(
          s"getBulkAvroSourceStream can't handle bulk format $format"
        )
    }
  }

  override def getAvroSourceStream[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      env: StreamExecutionEnvironment)(implicit
      fromKV: EmbeddedAvroRecordInfo[A] => E): DataStream[E] =
    if (format.isBulk) getBulkAvroSourceStream[E, A](env)
    else getTextAvroSourceStream[E, A](env)

}
