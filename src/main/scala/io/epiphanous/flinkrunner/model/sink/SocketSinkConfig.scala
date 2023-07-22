package io.epiphanous.flinkrunner.model.sink

import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.serde._
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.formats.csv.CsvRowSerializationSchema
import org.apache.flink.formats.json.JsonRowSerializationSchema
import org.apache.flink.streaming.api.functions.sink.SocketClientSink
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.types.Row

import java.nio.charset.StandardCharsets
import scala.util.{Failure, Success}

case class SocketSinkConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig)
    extends SinkConfig[ADT] {

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

  def getRowSerializationSchema(
      rowType: RowType): SerializationSchema[Row] = format match {
    case StreamFormatName.Json =>
      JsonRowSerializationSchema
        .builder()
        .withTypeInfo(InternalTypeInfo.of(rowType))
        .build()

    case StreamFormatName.Csv | StreamFormatName.Tsv |
        StreamFormatName.Psv | StreamFormatName.Delimited =>
      val delimitedConfig = DelimitedConfig.get(format, pfx(), config)
      val b               =
        new CsvRowSerializationSchema.Builder(InternalTypeInfo.of(rowType))
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

  override def _addRowSink(
      stream: DataStream[Row],
      rowType: RowType): Unit =
    _addSink[Row](stream, getRowSerializationSchema(rowType))

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
      serializationSchema: SerializationSchema[E]): Unit = {
    dataStream.addSink(
      new SocketClientSink[E](
        host,
        port,
        serializationSchema,
        maxRetries,
        autoFlush
      )
    )
    ()
  }
}
