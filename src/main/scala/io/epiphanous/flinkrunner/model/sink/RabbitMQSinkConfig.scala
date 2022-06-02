package io.epiphanous.flinkrunner.model.sink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkConnectorName.RabbitMQ
import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkConnectorName,
  FlinkEvent,
  RabbitMQConnectionInfo
}
import io.epiphanous.flinkrunner.serde.JsonSerializationSchema
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.rabbitmq.RMQSinkPublishOptions

import java.util.Properties

case class RabbitMQSinkConfig(
    config: FlinkConfig,
    connector: FlinkConnectorName = RabbitMQ,
    name: String,
    uri: String,
    useCorrelationId: Boolean,
    connectionInfo: RabbitMQConnectionInfo,
    queue: Option[String],
    properties: Properties)
    extends SinkConfig
    with LazyLogging {

  def getSerializationSchema[E <: FlinkEvent: TypeInformation]
      : SerializationSchema[E] = new JsonSerializationSchema[E](this)

  def getPublishOptions[E <: FlinkEvent: TypeInformation]
      : Option[RMQSinkPublishOptions[E]] = None
}
