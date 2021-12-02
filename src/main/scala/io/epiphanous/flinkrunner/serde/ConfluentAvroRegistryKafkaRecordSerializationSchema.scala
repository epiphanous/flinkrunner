package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.{
  KafkaAvroSerializer,
  KafkaAvroSerializerConfig
}
import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkEvent,
  KafkaSinkConfig
}
import io.epiphanous.flinkrunner.serde.ConfluentAvroRegistryKafkaRecordSerializationSchema.DEFAULT_CACHE_CAPACITY
import org.apache.avro.specific.SpecificRecord
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

import java.{lang, util}
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable

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

  val sinkConfig: KafkaSinkConfig =
    config.getSourceConfig(sinkName).asInstanceOf[KafkaSinkConfig]

  val url: String              =
    sinkConfig.properties.getProperty("schema.registry.url")
  val cacheCapacity: Int       = sinkConfig.properties
    .getProperty("schema.registry.cache.capacity", DEFAULT_CACHE_CAPACITY)
    .toInt
  val removeJavaProps: Boolean = sinkConfig.properties
    .getProperty("serializer.remove.java.props", "true")
    .toBoolean
  val useLogicalTypes: Boolean = sinkConfig.properties
    .getProperty("serializer.use.logical.type.converters", "true")
    .toBoolean

  /** create serializer config */
  val serializerConfig: util.Map[String, Boolean] = Map(
    KafkaAvroSerializerConfig.AVRO_REMOVE_JAVA_PROPS_CONFIG           -> removeJavaProps,
    KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG -> useLogicalTypes
  ).asJava

  /** our schema registry client */
  val schemaRegistryClient =
    new CachedSchemaRegistryClient(url, cacheCapacity)

  /** map to store the value, and optionally, key serializers */
  val serializers: mutable.Map[String, KafkaAvroSerializer] =
    mutable.Map(
      "value" -> new KafkaAvroSerializer(
        schemaRegistryClient,
        serializerConfig
      )
    )

  /** add the key serializer if needed */
  if (sinkConfig.isKeyed) {
    val keySerializer = new KafkaAvroSerializer(schemaRegistryClient)
    keySerializer.configure(serializerConfig, true)
    serializers += ("key" -> keySerializer)
  }

  /**
   * Convert an element into a producer record of byte arrays. Must be
   * defined by implementing classes.
   * @param element
   *   an instance of the flinkrunner ADT
   * @return
   *   ProducerRecord of bytes
   */
  def toProducerRecord(
      element: E): ProducerRecord[Array[Byte], Array[Byte]]

  override def serialize(
      element: E,
      context: KafkaRecordSerializationSchema.KafkaSinkContext,
      timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] =
    toProducerRecord(element)

}

object ConfluentAvroRegistryKafkaRecordSerializationSchema {
  val DEFAULT_CACHE_CAPACITY = "1000"
}
