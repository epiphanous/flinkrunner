package io.epiphanous.flinkrunner.serde

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.epiphanous.flinkrunner.model.source.KafkaSourceConfig
import io.epiphanous.flinkrunner.model.{
  EmbeddedAvroRecord,
  EmbeddedAvroRecordInfo,
  FlinkEvent
}
import io.epiphanous.flinkrunner.util.AvroUtils.parseSchemaString
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation

/** A deserialization schema that uses a confluent schema registry to
  * deserialize a kafka key/value pair into instances of a flink runner ADT
  * that also implements the EmbeddedAvroRecord trait.
  * @param sourceConfig
  *   config for the kafka source
  * @param schemaOpt
  *   optional avro schema string, which is required if A is GenericRecord
  * @param schemaRegistryClientOpt
  *   optional schema registry client (useful for testing)
  * @tparam E
  *   event type being deserialized, with an embedded avro record
  * @tparam A
  *   avro record type embedded within E
  * @tparam ADT
  *   flinkrunner algebraic data type
  */
class ConfluentAvroRegistryKafkaRecordDeserializationSchema[
    E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
    A <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent
](
    sourceConfig: KafkaSourceConfig[ADT]
)(implicit fromKV: EmbeddedAvroRecordInfo[A] => E)
    extends AvroRegistryKafkaRecordDeserializationSchema[E, A, ADT](
      sourceConfig
    ) {
  @transient
  lazy val schemaRegistryClient: SchemaRegistryClient =
    sourceConfig.schemaRegistryConfig.confluentClient

  @transient
  lazy val keyDeserializer
      : MyConfluentRegistryAvroDeserializationSchema[String] =
    MyConfluentRegistryAvroDeserializationSchema.get[String, ADT](
      clazz = classOf[String],
      reader = Some(Schema.create(Schema.Type.STRING)),
      schemaRegistryClient = schemaRegistryClient,
      sourceConfig = sourceConfig,
      isKey = true
    )

  @transient
  lazy val valueDeserializer
      : MyConfluentRegistryAvroDeserializationSchema[A] =
    MyConfluentRegistryAvroDeserializationSchema
      .get[A, ADT](
        clazz = avroClass,
        reader = sourceConfig.schemaOpt.map(parseSchemaString),
        schemaRegistryClient = schemaRegistryClient,
        sourceConfig = sourceConfig,
        isKey = false
      )
}
