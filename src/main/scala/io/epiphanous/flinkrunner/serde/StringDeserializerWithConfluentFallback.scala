package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.epiphanous.flinkrunner.serde.StringDeserializerWithConfluentFallback.CONFLUENT_MAGIC_BYTE
import org.apache.kafka.common.serialization.{
  Deserializer,
  StringDeserializer
}

/** A string key deserializer that falls back to confluent schema registry,
  * if configured.
  * @param confluentFallback
  *   an optional kafka avro deserializer to use if the key is confluent
  *   encoded
  */
class StringDeserializerWithConfluentFallback(
    confluentFallback: Option[KafkaAvroDeserializer] = None
) extends Deserializer[AnyRef]
    with LazyLogging {

  private val stringDeserializer = new StringDeserializer()

  private val confluentDeserializer: KafkaAvroDeserializer =
    confluentFallback.getOrElse(new KafkaAvroDeserializer())

  override def deserialize(topic: String, data: Array[Byte]): AnyRef = {
    if (
      Option(data).nonEmpty && data.headOption.contains(
        CONFLUENT_MAGIC_BYTE
      )
    ) {
      confluentDeserializer.deserialize(topic, data).toString
    } else
      stringDeserializer.deserialize(topic, data)
  }
}

object StringDeserializerWithConfluentFallback {
  final val CONFLUENT_MAGIC_BYTE = 0.toByte
}
