package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.model.FlinkConnectorName.Socket
import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkConnectorName,
  StreamFormatName
}
import io.epiphanous.flinkrunner.serde.RowDecoder
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.util.Properties

case class SocketSourceConfig(
    config: FlinkConfig,
    connector: FlinkConnectorName = Socket,
    name: String,
    host: String,
    port: Int,
    format: StreamFormatName,
    watermarkStrategy: String,
    maxAllowedLateness: Long,
    properties: Properties)
    extends SourceConfig {
  def getRowDecoder[E: TypeInformation]: RowDecoder[E] =
    RowDecoder.forEventType[E](format, properties)
}
