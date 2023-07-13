package io.epiphanous.flinkrunner.serde

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroDeserializerConfig}
import io.epiphanous.flinkrunner.model.FlinkEvent
import io.epiphanous.flinkrunner.model.source.KafkaSourceConfig
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecordBase
import org.apache.flink.formats.avro.SchemaCoder.SchemaCoderProvider
import org.apache.flink.formats.avro.registry.confluent.ConfluentSchemaRegistryCoder
import org.apache.flink.formats.avro.{RegistryAvroDeserializationSchema, SchemaCoder}

import java.util

/** A class to replace the built-in flink
  * ConfluentRegistryAvroDeserializationSchema which unfortunately hard
  * codes the confluent schema registry subject to null. This exists
  * because flink sends in a null subject value when interacting with
  * confluent's schema registry and that doesn't play nice with an
  * ecosystem where people expect a relationship between the subject name
  * and the schema name.
  * @param clazz
  *   class of avro record being deserialized (GenericRecord or an subclass
  *   of SpecificRecord)
  * @param schemaOpt
  *   an optional schema, provided when class is GenericRecord
  * @tparam A
  *   the avro record class
  */
class MyConfluentRegistryAvroDeserializationSchema[A](
    clazz: Class[A],
    schemaOpt: Option[Schema],
    schemaRegistryClient: SchemaRegistryClient,
    schemaRegistryConfigs: util.Map[String, String],
    subject: String,
    topic: String,
    isKey: Boolean)
    extends RegistryAvroDeserializationSchema[A](
      clazz,
      schemaOpt.orNull,
      new SchemaCoderProvider {
        override def get: SchemaCoder =
          new ConfluentSchemaRegistryCoder(subject, schemaRegistryClient)
      }
    ) {

  @transient lazy val deserializer: KafkaAvroDeserializer = {
    val kad = new KafkaAvroDeserializer(
      schemaRegistryClient
    )
    val p   = new util.HashMap[String, String]()
    p.putAll(schemaRegistryConfigs)
    p.put(
      KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,
      if (classOf[SpecificRecordBase].isAssignableFrom(clazz)) "true"
      else "false"
    )
    kad.configure(p, isKey)
    kad
  }

  override def deserialize(message: Array[Byte]): A =
    deserializer
      .deserialize(topic, message, schemaOpt.orNull)
      .asInstanceOf[A]

}

object MyConfluentRegistryAvroDeserializationSchema {
  def get[A, ADT <: FlinkEvent](
      clazz: Class[A],
      reader: Option[Schema] = None,
      schemaRegistryClient: SchemaRegistryClient,
      sourceConfig: KafkaSourceConfig[ADT],
      isKey: Boolean = false)
      : MyConfluentRegistryAvroDeserializationSchema[A] = {
    val topic   = sourceConfig.topic
    val subject = s"""$topic-${if (isKey) "key" else "value"}"""
    new MyConfluentRegistryAvroDeserializationSchema[A](
      clazz = clazz,
      schemaOpt = reader,
      schemaRegistryClient = schemaRegistryClient,
      schemaRegistryConfigs = sourceConfig.schemaRegistryConfig.props,
      subject = subject,
      topic = topic,
      isKey = isKey
    )
  }
}
