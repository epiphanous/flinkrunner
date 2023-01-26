package io.epiphanous.flinkrunner.model.sink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.serde._
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.formats.common.TimestampFormat
import org.apache.flink.formats.csv.CsvRowDataSerializationSchema
import org.apache.flink.formats.json.{
  JsonFormatOptions,
  JsonRowDataSerializationSchema
}
import org.apache.flink.streaming.api.functions.sink.SocketClientSink
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.logical.RowType

import java.nio.charset.StandardCharsets
import scala.util.{Failure, Success}

case class SocketSinkConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig)
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

  def getRowDataSerializationSchema(
      rowType: RowType): SerializationSchema[RowData] = format match {
    case StreamFormatName.Json                            =>
      new JsonRowDataSerializationSchema(
        rowType,
        TimestampFormat.ISO_8601,
        JsonFormatOptions.MapNullKeyMode.LITERAL,
        "null",
        true
      )
    case StreamFormatName.Csv | StreamFormatName.Tsv |
        StreamFormatName.Psv | StreamFormatName.Delimited =>
      val delimitedConfig = DelimitedConfig.get(format, pfx(), config)
      val b               = new CsvRowDataSerializationSchema.Builder(rowType)
        .setFieldDelimiter(delimitedConfig.columnSeparator)
        .setQuoteCharacter(delimitedConfig.quoteCharacter)
        .setEscapeCharacter(delimitedConfig.escapeCharacter)
      if (!delimitedConfig.useQuotes) b.disableQuoteCharacter()
      b.build()
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

  override def addRowSink(
      stream: DataStream[RowData],
      rowType: RowType): Unit =
    _addSink(stream, getRowDataSerializationSchema(rowType))
  override def addSink[E <: ADT: TypeInformation](
      dataStream: DataStream[E]): Unit =
    _addSink[E](dataStream, getSerializationSchema[E])

  def getAvroSerializationSchema[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation]: SerializationSchema[E] =
    new EmbeddedAvroJsonSerializationSchema[E, A, ADT](this)

  override def addAvroSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      dataStream: DataStream[E]): Unit =
    _addSink[E](dataStream, getAvroSerializationSchema[E, A])

  def _addSink[E](
      dataStream: DataStream[E],
      serializationSchema: SerializationSchema[E]): Unit =
    dataStream.addSink(
      new SocketClientSink[E](
        host,
        port,
        serializationSchema,
        maxRetries,
        autoFlush
      )
    )
}
