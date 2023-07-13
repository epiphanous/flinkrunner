package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
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
](sinkConfig: KafkaSinkConfig[ADT])
    extends AvroRegistryKafkaRecordSerializationSchema[E, A, ADT](
      sinkConfig
    )
    with LazyLogging {

  @transient
  lazy val serializer = new KafkaAvroSerializer(
    sinkConfig.schemaRegistryConfig.confluentClient
  )

}
