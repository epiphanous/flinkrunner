package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.model.sink.KafkaSinkConfig
import io.epiphanous.flinkrunner.model.{EmbeddedAvroRecord, FlinkEvent}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation

class EmbeddedAvroJsonKafkaRecordSerializationSchema[
    E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
    A <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent: TypeInformation](
    kafkaSinkConfig: KafkaSinkConfig[ADT])
    extends JsonKafkaRecordSerializationSchema[E, ADT](kafkaSinkConfig) {

  override val serializationSchema =
    new EmbeddedAvroJsonSerializationSchema[E, A, ADT](kafkaSinkConfig)
}
