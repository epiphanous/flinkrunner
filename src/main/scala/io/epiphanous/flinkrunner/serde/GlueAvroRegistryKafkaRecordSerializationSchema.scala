package io.epiphanous.flinkrunner.serde

import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer
import com.amazonaws.services.schemaregistry.serializers.avro.AWSKafkaAvroSerializer
import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.sink.KafkaSinkConfig
import io.epiphanous.flinkrunner.model.{EmbeddedAvroRecord, FlinkEvent}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.kafka.common.serialization.{Serializer, StringSerializer}

/** A serialization schema that uses an aws glue avro schema registry
  * client to serialize an instance of a flink runner ADT into kafka. The
  * flink runner ADT class must also extend the EmbeddedAvroRecord trait.
  * @param sinkConfig
  *   the kafka sink config
  */
case class GlueAvroRegistryKafkaRecordSerializationSchema[
    E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
    A <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent
](
    sinkConfig: KafkaSinkConfig[ADT]
) extends AvroRegistryKafkaRecordSerializationSchema[E, A, ADT](sinkConfig)
    with LazyLogging {

  @transient
  override lazy val keySerializer = new SimpleStringSerializer()

  @transient
  lazy val valueSerializer: AWSKafkaAvroSerializer = {
    val kas = new AWSKafkaAvroSerializer()
    // configure
    kas.configure(sinkConfig.schemaRegistryConfig.props, false)
    kas
  }

}
