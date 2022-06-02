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

import java.nio.charset.StandardCharsets
import java.util.Properties

case class FileSinkConfig(
    config: FlinkConfig,
    connector: FlinkConnectorName = File,
    name: String,
    path: String,
    isBulk: Boolean,
    format: Option[StreamFormatName],
    properties: Properties)
    extends SinkConfig
    with LazyLogging {

  def getFileSink[E <: FlinkEvent: TypeInformation]: FileSink[E] =
    if (isBulk) getBulkSink[E] else getRowSink[E]

  def getBulkSink[E <: FlinkEvent: TypeInformation]: FileSink[E] = {
    FileSink
      .forBulkFormat(
        new Path(path),
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

  def getRowSink[E <: FlinkEvent: TypeInformation]: FileSink[E] = {
    FileSink
      .forRowFormat(new Path(path), getRowEncoder[E])
      .withBucketAssigner(getBucketAssigner)
      .withBucketCheckInterval(getBucketCheckInterval)
      .withRollingPolicy(getCheckpointRollingPolicy)
      .withOutputFileConfig(getOutputFileConfig)
      .build()
  }

  def getBucketCheckInterval: Long =
    properties.getProperty("bucket.check.interval.ms", "60000").toLong

  def getBucketAssigner[E <: FlinkEvent]: BucketAssigner[E, String] = {
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
              context: BucketAssigner.Context): String = in.$key

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

  def getCheckpointRollingPolicy[E <: FlinkEvent]
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

  def getDelimitedConfig: DelimitedConfig = format match {
    case Some(StreamFormatName.Json) =>
      throw new RuntimeException(
        s"Invalid argument to getDelimitedConfig"
      )
    case Some(fmt)                   => DelimitedConfig.get(fmt, properties)
    case None                        => DelimitedConfig.CSV
  }

  def getRowEncoder[E: TypeInformation]: Encoder[E] = format match {
    case Some(StreamFormatName.Json) => new JsonFileEncoder[E]
    case _                           => new DelimitedFileEncoder[E](getDelimitedConfig)
  }

}
