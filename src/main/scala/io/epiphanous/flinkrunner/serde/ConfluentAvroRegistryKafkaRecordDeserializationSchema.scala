package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.{
  KafkaAvroDeserializer,
  KafkaAvroDeserializerConfig
}
import io.epiphanous.flinkrunner.model.{
  EmbeddedAvroRecord,
  FlinkConfig,
  FlinkEvent,
  KafkaSourceConfig
}
import org.apache.avro.generic.GenericContainer
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.util
import scala.reflect.{classTag, ClassTag}

/**
 * A deserialization schema that uses a confluent schema registry to
 * deserialize a kafka key/value pair into instances of a flink runner ADT
 * that also implements the [[EmbeddedAvroRecord]] trait.
 * @param sourceConfig
 *   config for the kafka source
 * @param config
 *   flink runner config
 */
class ConfluentAvroRegistryKafkaRecordDeserializationSchema[
    E <: FlinkEvent with EmbeddedAvroRecord: ClassTag](
    sourceConfig: KafkaSourceConfig,
    config: FlinkConfig,
    schemaRegistryClientOpt: Option[SchemaRegistryClient]
) extends KafkaRecordDeserializationSchema[E]
    with LazyLogging {

  val topic: String = sourceConfig.topic

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

  /**
   * Convert the key/value pair deserialized from kafka into an instances
   * of a flinkrunner event that implements [[EmbeddedAvroRecord]].
   * Implementors must provide a constructor for the flink event that takes
   * two arguments: an Option[String] key and an avro [[GenericContainer]]
   * value.
   * @param keyOpt
   *   An optional key
   * @param value
   *   a value
   * @return
   */
  def fromKV(keyOpt: Option[String], value: GenericContainer): E =
    classTag[E].runtimeClass
      .asInstanceOf[Class[E]]
      .getDeclaredConstructor()
      .newInstance(keyOpt, value)

  override def deserialize(
      record: ConsumerRecord[Array[Byte], Array[Byte]],
      out: Collector[E]): Unit = {
    val key   =
      keyDeserializer.map(ds =>
        ds.deserialize(topic, record.key()).toString
      )
    val value = valueDeserializer
      .deserialize(topic, record.value())
      .asInstanceOf[GenericContainer]
    if (Option(value).nonEmpty)
      out.collect(fromKV(key, value))
  }

  override def getProducedType: TypeInformation[E] =
    TypeInformation.of(new TypeHint[E] {})
}
