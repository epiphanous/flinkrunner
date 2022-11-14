package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.model.sink.SinkConfig
import io.epiphanous.flinkrunner.model.{EmbeddedAvroRecord, FlinkEvent}
import org.apache.avro.generic.GenericRecord
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.flink.api.common.typeinfo.TypeInformation

/** A json serialization schema for event types that embed an avro record.
  * The serialized json does not contain the avro and is not formatted as
  * an avro record (meaning, this serialization framework loses the avro
  * schema knowledge inherent in the event). And the serialized json only
  * contains data that is in the avro record.
  * @param sinkConfig
  *   config for the sink we're serializing to
  * @tparam E
  *   the event type member of the flink ADT
  * @tparam A
  *   the embedded avro type
  * @tparam ADT
  *   The flink event algebraic data type
  */
class EmbeddedAvroJsonSerializationSchema[
    E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
    A <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent](sinkConfig: SinkConfig[ADT])
    extends JsonSerializationSchema[E, ADT](sinkConfig) {

  val avroJsonEncoder = new EmbeddedAvroJsonFileEncoder[E, A, ADT]

  override def serialize(event: E): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    avroJsonEncoder.encode(event, baos)
    baos.toByteArray
  }
}
