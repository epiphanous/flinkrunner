package io.epiphanous.flinkrunner.serde

import com.fasterxml.jackson.core.{JsonFactory, JsonGenerator}
import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.{EmbeddedAvroRecord, FlinkEvent}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{EncoderFactory, JsonEncoder}
import org.apache.avro.specific.SpecificRecord
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.io.OutputStream
import java.nio.charset.StandardCharsets
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
    ADT <: FlinkEvent](schemaOpt: Option[Schema] = None)
    extends Encoder[E]
    with LazyLogging {

  @transient
  lazy val avroClass: Class[A] =
    implicitly[TypeInformation[A]].getTypeClass

  lazy val lineEndBytes: Array[Byte] =
    System.lineSeparator().getBytes(StandardCharsets.UTF_8)

  override def encode(element: E, stream: OutputStream): Unit = {
    val record  = element.$record
    val schema  = record.getSchema
    // is this inefficient?
    val encoder = EncoderFactory.get().jsonEncoder(schema, stream)
    val writer  = new GenericDatumWriter[GenericRecord](schema)

    Try {
      writer.write(record, encoder)
      encoder.flush()
      stream.write(lineEndBytes)
    }.fold(
      error =>
        logger.error(s"Failed to encode avro record $record", error),
      _ => ()
    )
  }
}
