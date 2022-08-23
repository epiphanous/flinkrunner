package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.{EmbeddedAvroRecord, FlinkEvent}
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.io.OutputStream
import scala.util.Try

/** A thin wrapper to emit an embedded avro record from events into an
  * output stream in a json (lines) format.
  *
  * @param pretty
  *   true if you want to indent the json code (default = false)
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
    ADT <: FlinkEvent]
    extends Encoder[E]
    with LazyLogging {

  override def encode(element: E, stream: OutputStream): Unit =
    encodeWithAvro(element, stream)

  def encodeWithAvro(element: E, stream: OutputStream): Unit = {
    val record  = element.$record
    val schema  = record.getSchema
    val encoder = EncoderFactory.get().jsonEncoder(schema, stream)
    val writer  = new GenericDatumWriter[A](schema)
    Try {
      writer.write(record, encoder)
      encoder.flush()
      stream.write(System.lineSeparator().getBytes())
    }.fold(
      error =>
        logger.error(s"Failed to encode avro record $record", error),
      _ => ()
    )
  }
}
