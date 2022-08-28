package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.model.FlinkConnectorName.RabbitMQ
import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkConnectorName, FlinkEvent, RabbitMQConnectionInfo}
import io.epiphanous.flinkrunner.serde.JsonRMQDeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.{Source, SourceSplit}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.rabbitmq.{RMQDeserializationSchema, RMQSource}

/** Source configuration for Rabbit MQ.
  * @param name
  *   unique name of the source
  * @param config
  *   The full flink config in which this source is defined
  * @param connector
  *   always RabbitMQ for this
  */
case class RabbitMQSourceConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig,
    connector: FlinkConnectorName = RabbitMQ)
    extends SourceConfig[ADT] {

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

  override def getSourceStream[E <: ADT: TypeInformation](
      env: StreamExecutionEnvironment): DataStream[E] =
    super.getSourceStream(env).setParallelism(1) // to ensure exactly once

}
