package io.epiphanous.flinkrunner.model.sink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model.{
  FlinkConnectorName,
  FlinkEvent,
  StreamFormatName
}
import io.epiphanous.flinkrunner.serde._
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.sink.SocketClientSink
import org.apache.flink.streaming.api.scala.DataStream

import java.nio.charset.StandardCharsets
import scala.util.{Failure, Success}

case class SocketSinkConfig[ADT <: FlinkEvent](
    name: String,
    runner: FlinkRunner[ADT])
    extends SinkConfig[ADT]
    with LazyLogging {

  override val connector: FlinkConnectorName = FlinkConnectorName.Socket

  val host: String             = config.getString(pfx("host"))
  val port: Int                = config.getInt(pfx("port"))
  val format: StreamFormatName = StreamFormatName.withNameInsensitive(
    config.getStringOpt(pfx("format")).getOrElse("csv")
  )
  val maxRetries: Int          = config.getIntOpt(pfx("max.retries")).getOrElse(0)
  val autoFlush: Boolean       =
    config.getBooleanOpt(pfx("auto.flush")).getOrElse(false)

  def getTextLineEncoder[E <: ADT: TypeInformation]: RowEncoder[E] =
    format match {
      case StreamFormatName.Json                            =>
        new JsonRowEncoder[E](JsonConfig(pfx(), config))
      case StreamFormatName.Csv | StreamFormatName.Tsv |
          StreamFormatName.Psv | StreamFormatName.Delimited =>
        new DelimitedRowEncoder[E](
          DelimitedConfig.get(format, pfx(), config)
        )
      case StreamFormatName.Parquet | StreamFormatName.Avro =>
        throw new RuntimeException(
          s"invalid format ${format.entryName} for socket sink $name"
        )
    }

  def getSerializationSchema[E <: ADT: TypeInformation]
      : SerializationSchema[E] =
    new SerializationSchema[E] {
      val encoder: RowEncoder[E]                = getTextLineEncoder[E]
      override def serialize(t: E): Array[Byte] =
        encoder.encode(t).map(_.getBytes(StandardCharsets.UTF_8)) match {
          case Success(bytes)     => bytes
          case Failure(exception) =>
            logger.error(exception.getMessage)
            null
        }
    }

  def getSink[E <: ADT: TypeInformation](
      dataStream: DataStream[E]): DataStreamSink[E] = dataStream.addSink(
    new SocketClientSink[E](
      host,
      port,
      getSerializationSchema[E],
      maxRetries,
      autoFlush
    )
  )
}
