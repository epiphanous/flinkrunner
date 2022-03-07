package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.epiphanous.flinkrunner.model.{FlinkConfig, KafkaSinkConfig}
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

import java.{lang, util}

/**
 * A schema to serialize an ADT event using a confluent avro schema
 * registry.
 */

/**
 * A serialization schema that uses a confluent avro schema registry client
 * to serialize an instance of a flink runner ADT into kafka. Implementing
 * classes must provide a `toKV` method to serialize a flink runner ADT
 * instance into a key/value pair to store in Kafka.
 * @param sinkConfig
 *   the kafka sink config
 * @param config
 *   flink runner config
 */
abstract case class ConfluentAvroRegistryKafkaRecordSerializationSchema[E](
    sinkConfig: KafkaSinkConfig,
    config: FlinkConfig
) extends KafkaRecordSerializationSchema[E]
    with LazyLogging {

  val schemaRegistryProps: util.HashMap[String, String] =
    config.schemaRegistryPropsForSink(sinkConfig)

  /** map to store the value, and optionally, key serializers */
  val valueSerializer = new KafkaAvroSerializer(
    config.schemaRegistryClient,
    schemaRegistryProps
  )

  /** add the key serializer if needed */
  val keySerializer: Option[KafkaAvroSerializer] =
    if (sinkConfig.isKeyed) {
      val ks = new KafkaAvroSerializer(config.schemaRegistryClient)
      ks.configure(schemaRegistryProps, true)
      Some(ks)
    } else None

  val topic: String = sinkConfig.topic

  /**
   * Convert a flink runner event instance into an optional key and
   * (required) value pair of types that match the schemas associated with
   * those types.
   * @param element
   *   E flink runner event instance
   * @return
   *   ([[Option]][ [[AnyRef]] ], [[AnyRef]] ) (optional key)/value pair to
   *   serialize into kafka
   */
  def toKV(element: E): (Option[AnyRef], AnyRef)

  /**
   * Return the event time associated with the element
   * @param element
   *   the event
   * @param timestamp
   *   a default processing timestamp if the implementor needs it
   * @return
   *   a long timestamp (milliseconds since epoch)
   */
  def eventTime(element: E, timestamp: Long): Long

  override def serialize(
      element: E,
      context: KafkaRecordSerializationSchema.KafkaSinkContext,
      timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val (k, v) = toKV(element)
    val key    =
      keySerializer.flatMap(ks => k.map(kk => ks.serialize(topic, kk)))
    val value  = valueSerializer.serialize(topic, v)
    new ProducerRecord(
      topic,
      null,
      eventTime(element, timestamp),
      key.orNull,
      value
    )
  }

}
