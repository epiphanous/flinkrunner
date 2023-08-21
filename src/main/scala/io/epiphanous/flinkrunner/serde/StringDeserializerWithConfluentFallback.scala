package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.{
  KafkaAvroDeserializer,
  KafkaAvroDeserializerConfig
}
import io.epiphanous.flinkrunner.serde.StringDeserializerWithConfluentFallback.CONFLUENT_MAGIC_BYTE
import org.apache.kafka.common.serialization.{
  Deserializer,
  StringDeserializer
}

import java.util

/** A string key deserializer that falls back to confluent schema registry,
  * if configured.
  * @param confluentFallback
  *   Either a configured schema registry client or a map of properties to
  *   configure a kafka avro deserializer. Defaults to an empty map (so no
  *   fallback).
  */
class StringDeserializerWithConfluentFallback(
    confluentFallback: Either[
      SchemaRegistryClient,
      util.Map[String, String]
    ] = Right(new util.HashMap()))
    extends Deserializer[AnyRef]
    with LazyLogging {

  private val stringDeserializer = new StringDeserializer()

  private def forceGeneric(
      kad: KafkaAvroDeserializer,
      props: java.util.Map[String, String] =
        new util.HashMap[String, String]()): KafkaAvroDeserializer = {
    val p = new util.HashMap[String, String]()
    if (!props.isEmpty) p.putAll(props)
    p.put(
      KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,
      "false"
    )
    kad.configure(p, true)
    kad
  }

  private val confluentDeserializer: Option[KafkaAvroDeserializer] =
    confluentFallback match {
      case Right(props) if !props.isEmpty =>
        Some(forceGeneric(new KafkaAvroDeserializer(), props))
      case Left(c)                        => Some(forceGeneric(new KafkaAvroDeserializer(c)))
      case _                              => None
    }

  override def deserialize(topic: String, data: Array[Byte]): AnyRef = {
    if (data.headOption.contains(CONFLUENT_MAGIC_BYTE)) {
      if (confluentDeserializer.isEmpty) logger.warn("deserializ")
      confluentDeserializer.map(_.deserialize(topic, data)).orNull
    } else
      stringDeserializer.deserialize(topic, data)
  }
}

object StringDeserializerWithConfluentFallback {
  final val CONFLUENT_MAGIC_BYTE = 0.toByte
}