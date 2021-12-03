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
import org.apache.avro.specific.SpecificRecord
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

import java.{lang, util}

/**
 * A schema to serialize an ADT event using a confluent avro schema
 * registry. An implementing class must provide a Flink
 * [[ConfluentRegistryAvroSerializationSchema]] to interface with the
 * schema registry. That registry is specific to a type that implements
 * Avro's [[SpecificRecord]] interface type.
 * @param sinkName
 *   name of the sink stream
 * @param config
 *   flink runner config
 * @tparam E
 *   the event type we are serializing from, which is a member of the ADT
 * @tparam ADT
 *   the flink runner ADT
 */
abstract class ConfluentAvroRegistryKafkaRecordSerializationSchema[
    E <: ADT,
    ADT <: FlinkEvent](
    sinkName: String,
    config: FlinkConfig[ADT]
) extends KafkaRecordSerializationSchema[E]
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

  /**
   * A helper method to serialize an arbitary key/value pair. This should
   * be used by subclasses that implement the [[toKeyValue()]] method.
   *
   * @param key
   *   the key
   * @param value
   *   the value
   * @tparam K
   *   the type of key
   * @tparam V
   *   the type of value
   * @return
   *   a tuple of byte arrays (with the key optional)
   */
//  def kvSerialize[K, V](key: K, value: V): (Array[Byte], Array[Byte]) = {
//    (
//      keySerializer.map(s => s.serialize(topic, key)).orNull,
//      valueSerializer.serialize(topic, value)
//    )
//  }

  /**
   * Implementing subclasses must provide an instance of a schema registry
   * client to use, for instance a <code>CachedSchemaRegistryClient</code>
   * or a <code>MockSchemaRegistryClient</code> for testing.
   */
  def schemaRegistryClient: SchemaRegistryClient

  /**
   * Convert a flink runner ADT instance into a key/value pair of objects
   * to serialize into a kafka message. This must be defined by
   * implementing subclasses.
   *
   * The purpose of this method is to decouple the structure of the flink
   * runner ADT from the avro schemas of the underlying kafka messages.
   *
   * @param element
   *   an instance of the flinkrunner ADT
   * @return
   *   (Option[AnyRef], AnyRef)
   */
  def toKeyValue(element: E): (Option[AnyRef], AnyRef)

  override def serialize(
      element: E,
      context: KafkaRecordSerializationSchema.KafkaSinkContext,
      timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val (k, v) = toKeyValue(element)
    val key    =
      keySerializer.flatMap(ks => k.map(kk => ks.serialize(topic, kk)))
    val value  = valueSerializer.serialize(topic, v)
    new ProducerRecord(topic, null, element.$timestamp, key.orNull, value)
  }

}
