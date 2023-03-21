package io.epiphanous.flinkrunner.model.sink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.serde._
import io.epiphanous.flinkrunner.util.AvroUtils.instanceOf
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase
import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.{
  BasePathBucketAssigner,
  DateTimeBucketAssigner
}
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.{
  CheckpointRollingPolicy,
  OnCheckpointRollingPolicy
}
import org.apache.flink.streaming.api.functions.sink.filesystem.{
  BucketAssigner,
  OutputFileConfig
}
import org.apache.flink.streaming.api.scala.DataStream

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

/** File sink config.
  *
  * Configuration:
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
  *   - `bucket`: optional configs associated with bucketing emitted files
  *     - `check.interval.ms`: maximum time to wait to emit the currently
  *       accumulating file
  *     - `assigner`: how files are assigned to buckets
  *       - `type`: one of (`none`, `datetime`, `custom`; default
  *         `datetime`)
  *       - `datetime.format`: when `type=datetime` this format specifier
  *         controls how the bucket path is created
  *   - `output.file.part`: controls the prefixing and suffixing of the
  *     emitted file names
  *     - `prefix`
  *     - `suffix`
  *
  * @param name
  *   name of the sink
  * @param config
  *   flinkrunner config
  * @tparam ADT
  *   the flinkrunner algebraic data type
  */
case class FileSinkConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig
) extends SinkConfig[ADT]
    with LazyLogging {

  override def connector: FlinkConnectorName = FlinkConnectorName.File

  val path: String      = config.getString(pfx("path"))
  val destination: Path = new Path(path)

  val format: StreamFormatName = StreamFormatName.withNameInsensitive(
    config.getString(pfx("format"))
  )

  val isBulk: Boolean      = format.isBulk
  val isText: Boolean      = format.isText
  val isDelimited: Boolean = format.isDelimited

  val delimitedConfig: DelimitedConfig =
    DelimitedConfig.get(format, pfx(), config)

  /** Create a row-encoded file sink and send the data stream to it.
    * @param dataStream
    *   the data stream of elements to send to the sink
    * @tparam E
    *   the type of elements in the outgoing data stream (member of the
    *   ADT)
    * @return
    *   DataStreamSink[E]
    */
  override def addSink[E <: ADT: TypeInformation](
      dataStream: DataStream[E]): Unit =
    dataStream.sinkTo(
      FileSink
        .forRowFormat(destination, getRowEncoder[E])
        .withBucketAssigner(getBucketAssigner)
        .withBucketCheckInterval(getBucketCheckInterval)
        .withRollingPolicy(getCheckpointRollingPolicy)
        .withOutputFileConfig(getOutputFileConfig)
        .build()
    )

  /** Create an bulk avro parquet file sink and send the data stream to it.
    * @param dataStream
    *   the data stream of elements to send to the sink
    * @tparam E
    *   the type of elements in the outgoing data stream (must be a member
    *   of the ADT that also implements EmbeddedAvroRecord[A])
    * @tparam A
    *   The type of avro record embedded in elements of type E
    * @return
    *   DataStream[E]
    */
  override def addAvroSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      dataStream: DataStream[E]): Unit = {
    val sink = format match {
      case StreamFormatName.Parquet | StreamFormatName.Avro =>
        FileSink
          .forBulkFormat(
            destination,
            new EmbeddedAvroWriterFactory[E, A, ADT](
              format == StreamFormatName.Parquet
            )
          )
          .withBucketAssigner(getBucketAssigner)
          .withBucketCheckInterval(getBucketCheckInterval)
          .withRollingPolicy(getCheckpointRollingPolicy)
          .withOutputFileConfig(getOutputFileConfig)
          .build()
      case StreamFormatName.Json | StreamFormatName.Csv |
          StreamFormatName.Tsv | StreamFormatName.Psv |
          StreamFormatName.Delimited =>
        FileSink
          .forRowFormat(destination, getAvroRowEncoder[E, A])
          .withBucketAssigner(getBucketAssigner)
          .withBucketCheckInterval(getBucketCheckInterval)
          .withRollingPolicy(getCheckpointRollingPolicy)
          .withOutputFileConfig(getOutputFileConfig)
          .build()
      case _                                                =>
        throw new RuntimeException(
          s"Invalid format for getAvroSink: $format"
        )
    }
    dataStream.sinkTo(sink)
  }

  def getBucketCheckInterval: Long =
    properties.getProperty("bucket.check.interval.ms", "60000").toLong

  def getBucketAssigner[E <: ADT]: BucketAssigner[E, String] = {
    properties.getProperty("bucket.assigner.type", "datetime") match {
      case "none"     => new BasePathBucketAssigner[E]()
      case "datetime" =>
        new DateTimeBucketAssigner[E](
          properties.getProperty(
            "bucket.assigner.datetime.format",
            "YYYY/MM/DD/HH"
          )
        )
      case "custom"   =>
        new BucketAssigner[E, String] {
          override def getBucketId(
              in: E,
              context: BucketAssigner.Context): String = in.$bucketId

          override def getSerializer: SimpleVersionedSerializer[String] =
            new SimpleVersionedSerializer[String] {
              override def getVersion: Int = 1

              override def serialize(e: String): Array[Byte] =
                e.getBytes(StandardCharsets.UTF_8)

              override def deserialize(
                  i: Int,
                  bytes: Array[Byte]): String = new String(bytes)
            }
        }
      case other      =>
        throw new IllegalArgumentException(
          s"Unknown bucket assigner type '$other'."
        )
    }
  }

  def getCheckpointRollingPolicy[E <: ADT]
      : CheckpointRollingPolicy[E, String] =
    OnCheckpointRollingPolicy.build()

  def getOutputFileConfig: OutputFileConfig = {
    val prefix = properties.getProperty("output.file.part.prefix", "part")
    val suffix = Option(properties.getProperty("output.file.part.suffix"))
    val ofc    = OutputFileConfig
      .builder()
      .withPartPrefix(prefix)
    suffix.map(ofc.withPartSuffix)
    ofc.build()
  }

  def getRowEncoder[E <: ADT: TypeInformation]: Encoder[E] = format match {
    case StreamFormatName.Json    => new JsonFileEncoder[E]
    case StreamFormatName.Parquet =>
      throw new RuntimeException(
        s"Invalid format for getRowEncoder: $format"
      )
    case _                        =>
      new DelimitedFileEncoder[E](delimitedConfig)
  }

  def getAvroRowEncoder[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation]: Encoder[E] = {
    val avroTypeClass = implicitly[TypeInformation[A]].getTypeClass
    format match {
      case StreamFormatName.Json                            =>
        new EmbeddedAvroJsonFileEncoder[E, A, ADT]()
      case StreamFormatName.Parquet | StreamFormatName.Avro =>
        throw new RuntimeException(
          s"${format.entryName} is a bulk format and invalid for encoding text to sink $name"
        )
      case _                                                =>
        val columns =
          if (classOf[SpecificRecordBase].isAssignableFrom(avroTypeClass))
            instanceOf(avroTypeClass).getSchema.getFields.asScala
              .map(_.name())
              .toList
          else
            properties
              .getProperty("column.names", "")
              .split("\\s+,\\s+")
              .toList
        new EmbeddedAvroDelimitedFileEncoder[E, A, ADT](
          delimitedConfig.copy(columns = columns)
        )
    }
  }

}
