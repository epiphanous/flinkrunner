package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.{EmbeddedAvroRecord, FlinkEvent}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.io.OutputStream

/** A JSON lines encoder for events with embedded avro records.
  *
  * @param jsonConfig
  *   json encoder config
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
    ADT <: FlinkEvent](jsonConfig: JsonConfig = JsonConfig())
    extends Encoder[E]
    with LazyLogging {

  @transient
  lazy val encoder = new JsonFileEncoder[A](jsonConfig)

  override def encode(element: E, stream: OutputStream): Unit =
    encoder.encode(element.$record, stream)

}
