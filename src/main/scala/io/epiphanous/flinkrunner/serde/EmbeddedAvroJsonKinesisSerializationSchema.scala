package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.model.sink.KinesisSinkConfig
import io.epiphanous.flinkrunner.model.{EmbeddedAvroRecord, FlinkEvent}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation

class EmbeddedAvroJsonKinesisSerializationSchema[
    E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
    A <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent: TypeInformation](
    kinesisSinkConfig: KinesisSinkConfig[ADT])
    extends JsonKinesisSerializationSchema[E, ADT](kinesisSinkConfig) {

  override val jsonSerializationSchema =
    new EmbeddedAvroJsonSerializationSchema[E, A, ADT](kinesisSinkConfig)
}
