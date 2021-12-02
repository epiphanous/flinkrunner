package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.{
  KafkaAvroDeserializer,
  KafkaAvroDeserializerConfig
}
import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkEvent,
  KafkaSourceConfig
}
import io.epiphanous.flinkrunner.serde.ConfluentAvroRegistryKafkaRecordDeserializationSchema.DEFAULT_CACHE_CAPACITY
import org.apache.avro.specific.SpecificRecord
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.util
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable

/**
 * A schema to deserialize bytes from kafka into an ADT event using a
 * confluent avro schema registry.
 *
 * @param sourceName
 *   name of the source stream
 * @param config
 *   flink runner config
 * @tparam E
 *   the event type we are producing here, which is a member of the ADT
 * @tparam ADT
 *   the flink runner ADT
 */
abstract class ConfluentAvroRegistryKafkaRecordDeserializationSchema[
    E <: ADT,
    ADT <: FlinkEvent
](
    sourceName: String,
    config: FlinkConfig[ADT]
) extends KafkaRecordDeserializationSchema[E]
    with LazyLogging {

  val sourceConfig: KafkaSourceConfig =
    config.getSourceConfig(sourceName).asInstanceOf[KafkaSourceConfig]

  val topic: String = sourceConfig.topic

  val url: String                    =
    sourceConfig.properties.getProperty("schema.registry.url")
  val cacheCapacity: Int             = sourceConfig.properties
    .getProperty("schema.registry.cache.capacity", DEFAULT_CACHE_CAPACITY)
    .toInt
  val useSpecificAvroReader: Boolean = sourceConfig.properties
    .getProperty("use.specific.avro.reader", "true")
    .toBoolean
  val useLogicalTypes: Boolean       = sourceConfig.properties
    .getProperty("use.logical.type.converters", "true")
    .toBoolean

  /** create deserializer config */
  val deserializerConfig: util.Map[String, Boolean] = Map(
    KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG             -> useSpecificAvroReader,
    KafkaAvroDeserializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG -> useLogicalTypes
  ).asJava

  /** our schema registry client */
  val schemaRegistryClient =
    new CachedSchemaRegistryClient(url, cacheCapacity)

  /** map to store the value, and optionally, key deserializers */
  val deserializers: mutable.Map[String, KafkaAvroDeserializer] =
    mutable.Map(
      "value" -> new KafkaAvroDeserializer(
        schemaRegistryClient,
        deserializerConfig
      )
    )

  /** add the key deserializer if needed */
  if (sourceConfig.isKeyed) {
    val keyDeserializer = new KafkaAvroDeserializer(schemaRegistryClient)
    keyDeserializer.configure(deserializerConfig, true)
    deserializers += ("key" -> keyDeserializer)
  }

  /**
   * Convert a kafka consumer record instance into an instance of our
   * produced event type. Must be defined by implementing classes.
   * @param record
   *   a kafka consumer record
   * @return
   *   an instance of the flink runner ADT
   */
  def fromConsumerRecord(
      record: ConsumerRecord[Array[Byte], Array[Byte]]): E

  override def deserialize(
      record: ConsumerRecord[Array[Byte], Array[Byte]],
      out: Collector[E]): Unit = fromConsumerRecord(record)

  override def getProducedType: TypeInformation[E] =
    TypeInformation.of(new TypeHint[E] {})
}

object ConfluentAvroRegistryKafkaRecordDeserializationSchema {
  val DEFAULT_CACHE_CAPACITY = "1000"
}
