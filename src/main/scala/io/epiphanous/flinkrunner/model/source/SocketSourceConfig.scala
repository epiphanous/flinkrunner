package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkConnectorName, FlinkEvent, StreamFormatName}
import io.epiphanous.flinkrunner.serde.{DelimitedConfig, DelimitedRowDecoder, JsonRowDecoder, RowDecoder}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/** A socket source configuration.
  * @param name
  *   name of the source
  * @param config
  *   flinkrunner config
  * @tparam ADT
  *   flinkrunner algebraic data type
  */
case class SocketSourceConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig
) extends SourceConfig[ADT] {

  override val connector: FlinkConnectorName = FlinkConnectorName.Socket

  val host: String             = config.getString(pfx("host"))
  val port: Int                = config.getInt(pfx("port"))
  val format: StreamFormatName = StreamFormatName.withNameInsensitive(
    config.getStringOpt(pfx("format")).getOrElse("json")
  )

  def getRowDecoder[E <: ADT: TypeInformation]: RowDecoder[E] =
    format match {
      case StreamFormatName.Json                            => new JsonRowDecoder[E]()
      case StreamFormatName.Csv | StreamFormatName.Tsv |
          StreamFormatName.Psv | StreamFormatName.Delimited =>
        new DelimitedRowDecoder[E](
          DelimitedConfig.get(format, pfx(), config)
        )
      case StreamFormatName.Parquet | StreamFormatName.Avro =>
        throw new RuntimeException(
          s"invalid format ${format.entryName} for socket source $name"
        )
    }

  override def getSourceStream[E <: ADT: TypeInformation](
      env: StreamExecutionEnvironment): DataStream[E] = {
    val decoder = getRowDecoder[E]
    env
      .socketTextStream(host, port)
      .name(s"raw:$label")
      .uid(s"raw:$label")
      .flatMap(line => decoder.decode(line))
      .uid(label)
      .name(label)
  }
}
