package io.epiphanous.flinkrunner.serde

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.{
  KafkaAvroDeserializer,
  KafkaAvroDeserializerConfig
}
import io.epiphanous.flinkrunner.model.source.KafkaSourceConfig
import io.epiphanous.flinkrunner.model.{
  EmbeddedAvroRecord,
  EmbeddedAvroRecordInfo,
  FlinkEvent
}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.util

/** A deserialization schema that uses a confluent schema registry to
  * deserialize a kafka key/value pair into instances of a flink runner ADT
  * that also implements the EmbeddedAvroRecord trait.
  * @param sourceConfig
  *   config for the kafka source
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
    sourceConfig: KafkaSourceConfig[ADT],
    schemaRegistryClientOpt: Option[SchemaRegistryClient] = None
)(implicit fromKV: EmbeddedAvroRecordInfo[A] => E)
    extends AvroRegistryKafkaRecordDeserializationSchema[E, A, ADT](
      sourceConfig
    ) {

  private def getDeserializer: KafkaAvroDeserializer =
    schemaRegistryClientOpt.fold(new KafkaAvroDeserializer())(c =>
      new KafkaAvroDeserializer(c)
    )

  def getConfig(isKey: Boolean): util.HashMap[String, String] = {
    val configs = new java.util.HashMap[String, String]()
    configs.putAll(sourceConfig.schemaRegistryConfig.props)
    if (isKey) {
      // force keys to be decoded generically
      configs.put(
        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,
        "false"
      )
    } else {

      /** If "specific.avro.reader" config is not set, then default it to
        * true if our embedded avro record class is a specific record.
        * Otherwise, rely on the setting provided in the config.
        */
      configs.putIfAbsent(
        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,
        avroClassIsSpecific.toString
      )
    }
    configs
  }

  @transient
  lazy val keyDeserializer: KafkaAvroDeserializer = {
    val kad = getDeserializer
    kad.configure(getConfig(true), true)
    kad
  }

  @transient
  lazy val valueDeserializer: KafkaAvroDeserializer = {
    val kad = getDeserializer
    kad.configure(getConfig(false), false)
    kad
  }

}
