package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.{
  KafkaAvroDeserializer,
  KafkaAvroDeserializerConfig
}
import io.epiphanous.flinkrunner.model.source.KafkaSourceConfig
import io.epiphanous.flinkrunner.model.{
  EmbeddedAvroRecord,
  FlinkConfig,
  FlinkEvent
}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.util

/**
 * A deserialization schema that uses a confluent schema registry to
 * deserialize a kafka key/value pair into instances of a flink runner ADT
 * that also implements the [[EmbeddedAvroRecord]] trait.
 * @param sourceConfig
 *   config for the kafka source
 * @param schemaRegistryClientOpt
 *   an optional schema registry client
 */
class ConfluentAvroRegistryKafkaRecordDeserializationSchema[
    E <: FlinkEvent with EmbeddedAvroRecord[A],
    A <: GenericRecord
](
    sourceConfig: KafkaSourceConfig,
    schemaRegistryClientOpt: Option[SchemaRegistryClient] = None
)(implicit fromKV: (Option[String], A) => E)
    extends KafkaRecordDeserializationSchema[E]
    with LazyLogging {

  val topic: String       = sourceConfig.topic
  val config: FlinkConfig = sourceConfig.config

  lazy val schemaRegistryProps: util.HashMap[String, String] =
    config.schemaRegistryPropsForSource(
      sourceConfig,
      Map(
        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG             -> "true",
        KafkaAvroDeserializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG -> "true"
      )
    )

  lazy val schemaRegistryClient: SchemaRegistryClient =
    schemaRegistryClientOpt.getOrElse(config.getSchemaRegistryClient)

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

  override def deserialize(
      record: ConsumerRecord[Array[Byte], Array[Byte]],
      out: Collector[E]): Unit = {
    val key =
      keyDeserializer.map(ds =>
        ds.deserialize(topic, record.key()).toString
      )
    valueDeserializer
      .deserialize(topic, record.value()) match {
      case a: GenericRecord        =>
        out.collect(fromKV(key, a.asInstanceOf[A]))
      case c if Option(c).nonEmpty =>
        throw new RuntimeException(
          s"deserialized value is wrong type of object"
        )
    }
  }

  override def getProducedType: TypeInformation[E] =
    TypeInformation.of(new TypeHint[E] {})
}
