package io.epiphanous.flinkrunner.model.sink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkConnectorName.File
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.serde._
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase
import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.streaming.api.datastream.DataStreamSink
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
import collection.JavaConverters._

case class FileSinkConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig,
    connector: FlinkConnectorName = File)
    extends SinkConfig[ADT]
    with LazyLogging {

  val path: String             = config.getString(pfx("path"))
  val destination: Path        = new Path(path)
  val format: StreamFormatName = StreamFormatName.withNameInsensitive(
    config.getString(pfx("format"))
  )
  val isBulk: Boolean          = StreamFormatName.isBulk(format)

  /** Create a row-encoded file sink and send the data stream to it.
    * @param dataStream
    *   the data stream of elements to send to the sink
    * @tparam E
    *   the type of elements in the outgoing data stream (member of the
    *   ADT)
    * @return
    *   DataStreamSink[E]
    */
  def getSink[E <: ADT: TypeInformation](
      dataStream: DataStream[E]): DataStreamSink[E] =
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
  def getAvroSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      dataStream: DataStream[E]): DataStreamSink[E] = {
    val sink = format match {
      case StreamFormatName.Parquet =>
        FileSink
          .forBulkFormat(
            destination,
            new EmbeddedAvroParquetRecordFactory[E, A, ADT]()
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
      case _                        =>
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
      new DelimitedFileEncoder[E](DelimitedConfig.get(format, properties))
  }

  def getAvroRowEncoder[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation]: Encoder[E] = {
    val avroTypeClass = implicitly[TypeInformation[A]].getTypeClass
    format match {
      case StreamFormatName.Json    =>
        new EmbeddedAvroJsonFileEncoder[E, A, ADT]()
      case StreamFormatName.Parquet =>
        throw new RuntimeException(
          s"Parquet is a bulk format and invalid for getAvroRowEncoder on sink $name"
        )
      case _                        =>
        val columns =
          if (classOf[SpecificRecordBase].isAssignableFrom(avroTypeClass))
            avroTypeClass
              .getConstructor()
              .newInstance()
              .getSchema
              .getFields
              .asScala
              .map(_.name())
              .toList
          else
            properties
              .getProperty("column.names", "")
              .split("\\s+,\\s+")
              .toList
        new EmbeddedAvroDelimitedFileEncoder[E, A, ADT](
          DelimitedConfig.get(format, properties, columns)
        )
    }
  }

}
