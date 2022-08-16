package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.model.{EmbeddedAvroRecord, FlinkEvent}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.io.OutputStream

/** A thin wrapper to emit an embedded avro record from events into an
  * output stream in a json (lines) format.
  *
  * @param pretty
  *   true if you want to indent the json code (default = false)
  * @param sortKeys
  *   true if you want to sort the fields by their names (default = false)
  * @tparam E
  *   the ADT event type that embeds an avro record of type A
  * @tparam A
  *   the avro type embedded in the ADT event
  * @tparam ADT
  *   the flink event algebraic data type
  */
class EmbeddedAvroJsonFileEncoder[
    E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
    A <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent](pretty: Boolean = false, sortKeys: Boolean = false)
    extends Encoder[E] {

  @transient
  lazy val avroJsonFileEncoder = new JsonFileEncoder[A](pretty, sortKeys)

  override def encode(element: E, stream: OutputStream): Unit =
    avroJsonFileEncoder.encode(element.$record, stream)
}
