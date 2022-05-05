package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.epiphanous.flinkrunner.model.{FlinkConfig, KafkaSourceConfig}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.util

/**
 * A deserialization schema that uses a confluent schema registry to
 * deserialize a kafka key/value pair into instances of a flink runner ADT.
 * Implementing classes must provide a mapping `fromKV` method to create a
 * sequence of zero or more flink runner ADT instances from the key/value
 * pair deserialized from Kafka.
 * @param sourceConfig
 *   config for the kafka source
 * @param config
 *   flink runner config
 */
abstract class ConfluentAvroRegistryKafkaRecordDeserializationSchema[E](
    sourceConfig: KafkaSourceConfig,
    config: FlinkConfig
) extends KafkaRecordDeserializationSchema[E]
    with LazyLogging {

  val topic: String = sourceConfig.topic

  lazy val schemaRegistryProps: util.HashMap[String, String] =
    config.schemaRegistryPropsForSource(sourceConfig)

  lazy val schemaRegistryClient: SchemaRegistryClient =
    config.getSchemaRegistryClient

  lazy val valueDeserializer = new KafkaAvroDeserializer(
    schemaRegistryClient,
    schemaRegistryProps
  )

  lazy val keyDeserializer: Option[KafkaAvroDeserializer] =
    if (sourceConfig.isKeyed) {
      val ks = new KafkaAvroDeserializer(schemaRegistryClient)
      ks.configure(schemaRegistryProps, true)
      Some(ks)
    } else None

  /**
   * Convert the key/value pair deserialized from kafka into zero or more
   * instances of a flinkrunner event. All the types here are [[AnyRef]]
   * because there is no good way to force the types to be consistent with
   * what's in Kafka, but only those objects returned that are in fact
   * instances of type `E` will be passed out of the collector to the
   * stream.
   * @param keyOpt
   *   An optional key
   * @param value
   *   a value
   * @return
   */
  def fromKV(keyOpt: Option[AnyRef], value: AnyRef): Seq[AnyRef]

  override def deserialize(
      record: ConsumerRecord[Array[Byte], Array[Byte]],
      out: Collector[E]): Unit = {
    val key   =
      keyDeserializer.map(ds => ds.deserialize(topic, record.key()))
    val value = valueDeserializer.deserialize(topic, record.value())
    if (Option(value).nonEmpty)
      fromKV(key, value)
        .filter(_.isInstanceOf[E])
        .map(_.asInstanceOf[E])
        .foreach(out.collect)
  }

  override def getProducedType: TypeInformation[E] =
    TypeInformation.of(new TypeHint[E] {})
}
