package io.epiphanous.flinkrunner.model.sink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkConnectorName.Kafka
import io.epiphanous.flinkrunner.model.{
  EmbeddedAvroRecord,
  FlinkConfig,
  FlinkConnectorName,
  FlinkEvent
}
import io.epiphanous.flinkrunner.serde.{
  ConfluentAvroRegistryKafkaRecordSerializationSchema,
  JsonKafkaRecordSerializationSchema
}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema

import java.util.Properties

case class KafkaSinkConfig(
    config: FlinkConfig,
    connector: FlinkConnectorName = Kafka,
    name: String,
    topic: String,
    isKeyed: Boolean,
    bootstrapServers: String,
    deliveryGuarantee: DeliveryGuarantee,
    properties: Properties)
    extends SinkConfig
    with LazyLogging {

  /** Return an confluent avro serialization schema */
  def getAvroSerializationSchema[
      E <: FlinkEvent with EmbeddedAvroRecord[A],
      A <: GenericRecord]: KafkaRecordSerializationSchema[E] = {
    new ConfluentAvroRegistryKafkaRecordSerializationSchema[E, A](
      this
    )
  }

  /** Returns, by default, a json serialization schema */
  def getSerializationSchema[E <: FlinkEvent: TypeInformation]
      : KafkaRecordSerializationSchema[E] =
    new JsonKafkaRecordSerializationSchema[E](this)

}
