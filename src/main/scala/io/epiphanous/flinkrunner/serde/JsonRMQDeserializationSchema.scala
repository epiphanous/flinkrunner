package io.epiphanous.flinkrunner.serde

import com.rabbitmq.client.{AMQP, Envelope}
import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkEvent
import io.epiphanous.flinkrunner.model.source.RabbitMQSourceConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.rabbitmq.RMQDeserializationSchema

class JsonRMQDeserializationSchema[E <: FlinkEvent: TypeInformation](
    rabbitMQSourceConfig: RabbitMQSourceConfig)
    extends RMQDeserializationSchema[E]
    with LazyLogging {
  val deserializationSchema =
    new JsonDeserializationSchema[E](rabbitMQSourceConfig)
  override def deserialize(
      envelope: Envelope,
      properties: AMQP.BasicProperties,
      body: Array[Byte],
      collector: RMQDeserializationSchema.RMQCollector[E]): Unit = {
    val element = deserializationSchema.deserialize(body)
    if (element != null) collector.collect(element)
  }

  override def isEndOfStream(nextElement: E): Boolean = false

  override def getProducedType: TypeInformation[E] =
    deserializationSchema.getProducedType
}
