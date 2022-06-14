package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkConnectorName,
  FlinkEvent,
  StreamFormatName
}
import io.epiphanous.flinkrunner.serde.RowDecoder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{
  DataStream,
  StreamExecutionEnvironment
}

case class SocketSourceConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig,
    connector: FlinkConnectorName = FlinkConnectorName.Socket)
    extends SourceConfig[ADT] {

  val host: String             = config.getString(pfx("host"))
  val port: Int                = config.getInt(pfx("port"))
  val format: StreamFormatName = StreamFormatName.withNameInsensitive(
    config.getStringOpt(pfx("format")).getOrElse("csv")
  )

  def getRowDecoder[E <: ADT: TypeInformation]: RowDecoder[E] =
    RowDecoder.forEventType[E](format, properties)

  override def getSourceStream[E <: ADT: TypeInformation](
      env: StreamExecutionEnvironment): DataStream[E] = {
    val decoder = getRowDecoder[E]
    env
      .socketTextStream(host, port)
      .name(s"raw:$label")
      .uid(s"raw:$label")
      .flatMap(line => decoder.decode(line).toOption)
      .uid(label)
      .name(label)
  }
}
