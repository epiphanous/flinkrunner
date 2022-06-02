package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkEvent
import io.epiphanous.flinkrunner.model.sink.KafkaSinkConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

import java.lang
import java.nio.charset.StandardCharsets

class JsonKafkaRecordSerializationSchema[E <: FlinkEvent: TypeInformation](
    kafkaSinkConfig: KafkaSinkConfig)
    extends KafkaRecordSerializationSchema[E]
    with LazyLogging {
  val serializationSchema =
    new JsonSerializationSchema[E](kafkaSinkConfig)

  override def serialize(
      element: E,
      context: KafkaRecordSerializationSchema.KafkaSinkContext,
      timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val key   =
      if (kafkaSinkConfig.isKeyed)
        element.$key.getBytes(StandardCharsets.UTF_8)
      else null
    val value = serializationSchema.serialize(element)
    new ProducerRecord[Array[Byte], Array[Byte]](
      kafkaSinkConfig.topic,
      null,
      element.$timestamp,
      key,
      value
    )
  }
}
