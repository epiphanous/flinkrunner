package io.epiphanous.flinkrunner.model.sink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkConnectorName.File
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.serde.{
  DelimitedConfig,
  DelimitedFileEncoder,
  JsonFileEncoder
}
import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.formats.parquet.avro.AvroParquetWriters
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

  def getSink[E <: ADT: TypeInformation](
      dataStream: DataStream[E]): DataStreamSink[E] =
    dataStream.sinkTo(if (isBulk) getBulkSink[E] else getRowSink[E])

  def getBulkSink[E <: ADT: TypeInformation]: FileSink[E] = {
    FileSink
      .forBulkFormat(
        destination,
        AvroParquetWriters.forReflectRecord(
          implicitly[TypeInformation[E]].getTypeClass
        )
      )
      .withBucketAssigner(getBucketAssigner)
      .withBucketCheckInterval(getBucketCheckInterval)
      .withRollingPolicy(getCheckpointRollingPolicy)
      .withOutputFileConfig(getOutputFileConfig)
      .build()
  }

  def getRowSink[E <: ADT: TypeInformation]: FileSink[E] = {
    FileSink
      .forRowFormat(destination, getRowEncoder[E])
      .withBucketAssigner(getBucketAssigner)
      .withBucketCheckInterval(getBucketCheckInterval)
      .withRollingPolicy(getCheckpointRollingPolicy)
      .withOutputFileConfig(getOutputFileConfig)
      .build()
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

}
