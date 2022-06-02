package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.model.FlinkConnectorName.RabbitMQ
import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkConnectorName,
  FlinkEvent,
  RabbitMQConnectionInfo
}
import io.epiphanous.flinkrunner.serde.JsonRMQDeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.rabbitmq.RMQDeserializationSchema

import java.util.Properties

/**
 * Source configuration for Rabbit MQ.
 * @param config
 *   The full flink config in which this source is defined
 * @param connector
 *   always RabbitMQ for this
 * @param name
 *   unique name of the source
 * @param uri
 *   the uri of the connection
 * @param useCorrelationId
 *   true if the source queue uses correlation ids
 * @param queue
 *   name of the source queue
 * @param watermarkStrategy
 *   a strategy for applying watermarks to the event stream
 * @param maxAllowedLateness
 *   a duration defining when late arriving messages are dropped
 * @param connectionInfo
 *   rabbit mq connection info object (constructed from other properties
 *   defined in this source configuration)
 * @param properties
 *   a java properties object for other configs not specified above
 */
case class RabbitMQSourceConfig(
    config: FlinkConfig,
    connector: FlinkConnectorName = RabbitMQ,
    name: String,
    uri: String,
    useCorrelationId: Boolean,
    queue: String,
    watermarkStrategy: String,
    maxAllowedLateness: Long,
    connectionInfo: RabbitMQConnectionInfo,
    properties: Properties)
    extends SourceConfig {

  /**
   * Return a deserialization schema for rabbit mq. This implementation
   * assumes a JSON fomatted messages in rabbit.
   * @tparam E
   *   event type
   * @return
   *   RMQDeserializationSchema[E]
   */
  def getDeserializationSchema[E <: FlinkEvent: TypeInformation]
      : RMQDeserializationSchema[E] =
    new JsonRMQDeserializationSchema[E](this)

}
