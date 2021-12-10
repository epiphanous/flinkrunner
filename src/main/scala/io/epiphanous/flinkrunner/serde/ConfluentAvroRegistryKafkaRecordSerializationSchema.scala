package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkConnectorName,
  FlinkEvent,
  KafkaSinkConfig
}
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

import java.{lang, util}

/**
 * A schema to serialize an ADT event using a confluent avro schema
 * registry.
 */

/**
 * A serialization schema that uses the provided confluent avro schema
 * registry client to serialize an instance of a flink runner ADT into
 * kafka. In order to decouple the shape of the flink runner ADT types from
 * the types that are serialized in kafka, and to support providing both a
 * key and a value to kafka, a user of this class must provide a `toKV`
 * partial function that maps from the flink runner ADT instance to the
 * specific key and value pair (key is optional) that will be serialized
 * into the kafka sink.
 *
 * Usually, `toKV` is as simple as providing a set of cases like so, where
 * `A` and `B` are subclasses of the flink runner ADT.
 * {{{
 *   {
 *   //               key        value
 *     case a:A => (Some(a.id), a.value)
 *     case b:B => (Some(b.id), b.value)
 *   }
 * }}}
 * @param sinkName
 *   name of the kafka sink we serialize into
 * @param config
 *   flink runner config
 * @param schemaRegistryClient
 *   an instance of a confluent schema registry client to use, for instance
 *   a <code>CachedSchemaRegistryClient</code> or a
 *   <code>MockSchemaRegistryClient</code> for testing
 * @param toKV
 *   a partial function that maps an instance of the flink runner ADT into
 *   a key/value pair that will be serialized into the kafka sink
 * @tparam ADT
 *   the flink runner ADT type
 */
case class ConfluentAvroRegistryKafkaRecordSerializationSchema[
    ADT <: FlinkEvent](
    sinkName: String,
    config: FlinkConfig[ADT],
    schemaRegistryClient: SchemaRegistryClient,
    toKV: PartialFunction[ADT, (Option[AnyRef], AnyRef)]
) extends KafkaRecordSerializationSchema[ADT]
    with LazyLogging {

  val sinkConfig: KafkaSinkConfig = {
    val sc = config.getSinkConfig(sinkName)
    if (sc.connector != FlinkConnectorName.Kafka)
      throw new RuntimeException(
        s"Requested sink $sinkName is not a kafka sink"
      )
    sc.asInstanceOf[KafkaSinkConfig]
  }

  val schemaRegistryProps: util.HashMap[String, String] =
    sinkConfig.propertiesMap

  /** map to store the value, and optionally, key serializers */
  val valueSerializer = new KafkaAvroSerializer(
    schemaRegistryClient,
    schemaRegistryProps
  )

  /** add the key serializer if needed */
  val keySerializer: Option[KafkaAvroSerializer] =
    if (sinkConfig.isKeyed) {
      val ks = new KafkaAvroSerializer(schemaRegistryClient)
      ks.configure(schemaRegistryProps, true)
      Some(ks)
    } else None

  val topic: String = sinkConfig.topic

  override def serialize(
      element: ADT,
      context: KafkaRecordSerializationSchema.KafkaSinkContext,
      timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val (k, v) = toKV(element)
    val key    =
      keySerializer.flatMap(ks => k.map(kk => ks.serialize(topic, kk)))
    val value  = valueSerializer.serialize(topic, v)
    new ProducerRecord(topic, null, element.$timestamp, key.orNull, value)
  }

}
