package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.epiphanous.flinkrunner.model.sink.KafkaSinkConfig
import io.epiphanous.flinkrunner.model.{EmbeddedAvroRecord, FlinkEvent}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation

/** A serialization schema that uses a confluent avro schema registry
  * client to serialize an instance of a flink runner ADT into kafka. The
  * flink runner ADT class must also extend the EmbeddedAvroRecord trait.
  * @param sinkConfig
  *   the kafka sink config
  */
case class ConfluentAvroRegistryKafkaRecordSerializationSchema[
    E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
    A <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent
](
    sinkConfig: KafkaSinkConfig[ADT],
    schemaRegistryClientOpt: Option[SchemaRegistryClient] = None)
    extends AvroRegistryKafkaRecordSerializationSchema[E, A, ADT](
      sinkConfig
    )
    with LazyLogging {

  def getSerializer: KafkaAvroSerializer =
    schemaRegistryClientOpt.fold(new KafkaAvroSerializer())(c =>
      new KafkaAvroSerializer(c)
    )

  @transient override lazy val keySerializer: KafkaAvroSerializer = {
    val kas = getSerializer
    kas.configure(sinkConfig.schemaRegistryConfig.props, true)
    kas
  }

  @transient
  lazy val valueSerializer: KafkaAvroSerializer = {
    val kas = getSerializer
    kas.configure(sinkConfig.schemaRegistryConfig.props, false)
    kas
  }

}
