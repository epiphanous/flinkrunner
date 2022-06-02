package io.epiphanous.flinkrunner.model.sink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkConnectorName.Socket
import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkConnectorName,
  StreamFormatName
}
import io.epiphanous.flinkrunner.serde.RowEncoder
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.nio.charset.StandardCharsets
import java.util.Properties
import scala.util.{Failure, Success}

case class SocketSinkConfig(
    config: FlinkConfig,
    connector: FlinkConnectorName = Socket,
    name: String,
    host: String,
    port: Int,
    format: StreamFormatName,
    maxRetries: Option[Int] = None,
    autoFlush: Option[Boolean] = None,
    properties: Properties)
    extends SinkConfig
    with LazyLogging {
  def getTextLineEncoder[E: TypeInformation]: RowEncoder[E] =
    RowEncoder.forEventType[E](format, properties)

  def getSerializationSchema[E: TypeInformation]: SerializationSchema[E] =
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
}
