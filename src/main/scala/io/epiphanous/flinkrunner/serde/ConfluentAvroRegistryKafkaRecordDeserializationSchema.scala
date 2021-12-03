package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkConnectorName,
  FlinkEvent,
  KafkaSourceConfig
}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.util

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

  val sourceConfig: KafkaSourceConfig = {
    val sc = config.getSourceConfig(sourceName)
    if (sc.connector != FlinkConnectorName.Kafka)
      throw new RuntimeException(
        s"Requested source $sourceName is not a kafka source"
      )
    sc.asInstanceOf[KafkaSourceConfig]
  }

  val schemaRegistryProps: util.HashMap[String, String] =
    sourceConfig.propertiesMap

  val topic: String = sourceConfig.topic

  /**
   * Implementing subclasses must provide an instance of a schema registry
   * client to use, for instance a <code>CachedSchemaRegistryClient</code>
   * or a <code>MockSchemaRegistryClient</code> for testing.
   */
  def schemaRegistryClient: SchemaRegistryClient

  val valueDeserializer = new KafkaAvroDeserializer(
    schemaRegistryClient,
    schemaRegistryProps
  )

  val keyDeserializer: Option[KafkaAvroDeserializer] =
    if (sourceConfig.isKeyed) {
      val ks = new KafkaAvroDeserializer(schemaRegistryClient)
      ks.configure(schemaRegistryProps, true)
      Some(ks)
    } else None

  /**
   * Convert a deserialized key/value pair of objects into an instance of
   * the flink runner ADT. This method must be implemented by subclasses.
   *
   * The key and value are passed as AnyRefs, so implementing subclasses
   * will need to pattern match.
   *
   * @param key
   *   an optional deserialized key object
   * @param value
   *   a deserialized value object
   * @return
   *   an instance of the flink runner ADT
   */
  def fromKeyValue(key: Option[AnyRef], value: AnyRef): E

  override def deserialize(
      record: ConsumerRecord[Array[Byte], Array[Byte]],
      out: Collector[E]): Unit = {
    val key   =
      keyDeserializer.map(ds => ds.deserialize(topic, record.key()))
    val value = valueDeserializer.deserialize(topic, record.value())
    if (Option(value).nonEmpty) out.collect(fromKeyValue(key, value))
  }

  override def getProducedType: TypeInformation[E] =
    TypeInformation.of(new TypeHint[E] {})
}
