package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.model.FlinkConnectorName.Kafka
import io.epiphanous.flinkrunner.model.{
  EmbeddedAvroRecord,
  FlinkConfig,
  FlinkConnectorName,
  FlinkEvent
}
import io.epiphanous.flinkrunner.serde.{
  ConfluentAvroRegistryKafkaRecordDeserializationSchema,
  JsonKafkaRecordDeserializationSchema
}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema

import java.util.Properties

case class KafkaSourceConfig(
    config: FlinkConfig,
    connector: FlinkConnectorName = Kafka,
    name: String,
    topic: String,
    isKeyed: Boolean,
    bootstrapServers: String,
    watermarkStrategy: String,
    maxAllowedLateness: Long,
    bounded: Boolean = false,
    startingOffsets: OffsetsInitializer,
    stoppingOffsets: OffsetsInitializer,
    properties: Properties)
    extends SourceConfig {

  /**
   * Returns a confluent avro registry aware deserialization schema for
   * kafka.
   *
   * @param fromKV
   *   an implicit method that creates an event from an optional key and
   *   avro record value
   * @tparam E
   *   the stream element type (must mix in EmbeddedAvroRecord[A] trait)
   * @tparam A
   *   the avro record type
   * @return
   *   KafkaRecordDeserializationSchema[E]
   */
  def getAvroDeserializationSchema[
      E <: FlinkEvent with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord](implicit fromKV: (Option[String], A) => E)
      : KafkaRecordDeserializationSchema[E] = {
    new ConfluentAvroRegistryKafkaRecordDeserializationSchema[E, A](
      this
    )
  }

  /**
   * Return a deserialization schema for kafka. This defaults to return a
   * JSON deserialization schema.
   *
   * @tparam E
   *   the event type
   * @return
   *   KafkaRecordDeserializationSchema[E]
   */
  def getDeserializationSchema[E <: FlinkEvent: TypeInformation]
      : KafkaRecordDeserializationSchema[E] =
    new JsonKafkaRecordDeserializationSchema[E](this)
}
