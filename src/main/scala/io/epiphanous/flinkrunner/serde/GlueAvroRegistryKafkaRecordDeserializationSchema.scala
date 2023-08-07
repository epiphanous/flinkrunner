package io.epiphanous.flinkrunner.serde

import com.amazonaws.services.schemaregistry.deserializers.avro.AWSKafkaAvroDeserializer
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.epiphanous.flinkrunner.model.source.KafkaSourceConfig
import io.epiphanous.flinkrunner.model.{
  EmbeddedAvroRecord,
  EmbeddedAvroRecordInfo,
  FlinkEvent
}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation

/** A flink deserialization schema that uses an aws glue avro schema
  * registry to deserialize a kafka consumer record into an embedded avro
  * event instance. It uses a [[StringDeserializerWithConfluentFallback]]
  * key deserializer.
  * @param sourceConfig
  *   config for the kafka source
  * @param fromKV
  *   implicitly provided method to create an instance of event type E from
  *   its embedded avro record
  * @tparam E
  *   event type being deserialized, with an embedded avro record
  * @tparam A
  *   avro record type embedded within E
  * @tparam ADT
  *   flinkrunner algebraic data type
  */
class GlueAvroRegistryKafkaRecordDeserializationSchema[
    E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
    A <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent](
    sourceConfig: KafkaSourceConfig[ADT],
    schemaRegistryClientOpt: Option[SchemaRegistryClient] = None
)(implicit fromKV: EmbeddedAvroRecordInfo[A] => E)
    extends AvroRegistryKafkaRecordDeserializationSchema[E, A, ADT](
      sourceConfig
    ) {

  @transient
  override lazy val keyDeserializer =
    new StringDeserializerWithConfluentFallback(
      schemaRegistryClientOpt
        .map(c => Left(c))
        .getOrElse(Right(sourceConfig.schemaRegistryConfig.confluentProps))
    )

  @transient
  override lazy val valueDeserializer: AWSKafkaAvroDeserializer = {
    val kad = new AWSKafkaAvroDeserializer()
    // configure
    kad.configure(sourceConfig.schemaRegistryConfig.props, false)
    kad
  }

}
