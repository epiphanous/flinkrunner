package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkConnectorName,
  FlinkEvent,
  RabbitMQConnectionInfo
}
import io.epiphanous.flinkrunner.serde.JsonRMQDeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.{Source, SourceSplit}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.connectors.rabbitmq.{
  RMQDeserializationSchema,
  RMQSource
}

/** Source configuration for Rabbit MQ.
  * @param name
  *   name of the source
  * @param config
  *   flinkrunner config
  * @tparam ADT
  *   flinkrunner algebraic data type
  */
case class RabbitMQSourceConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig
) extends SourceConfig[ADT] {

  override val connector: FlinkConnectorName = FlinkConnectorName.RabbitMQ

  override lazy val parallelism: Int = 1 // ensure exactly once

  val uri: String               = config.getString(pfx("uri"))
  val useCorrelationId: Boolean =
    config.getBoolean(pfx("use.correlation.id"))
  val queue: String             = config.getString(pfx("queue"))

  val connectionInfo: RabbitMQConnectionInfo =
    RabbitMQConnectionInfo(uri, properties)

  /** Return a deserialization schema for rabbit mq. This implementation
    * assumes a JSON fomatted messages in rabbit.
    * @tparam E
    *   event type
    * @return
    *   RMQDeserializationSchema[E]
    */
  def getDeserializationSchema[E <: ADT: TypeInformation]
      : RMQDeserializationSchema[E] =
    new JsonRMQDeserializationSchema[E, ADT](this)

  override def getSource[E <: ADT: TypeInformation]
      : Either[SourceFunction[E], Source[E, _ <: SourceSplit, _]] =
    Left(
      new RMQSource[E](
        connectionInfo.rmqConfig,
        queue,
        useCorrelationId,
        getDeserializationSchema
      )
    )

}
