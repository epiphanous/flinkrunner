package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.{EmbeddedAvroRecord, FlinkEvent}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{EncoderFactory, JsonEncoder}
import org.apache.avro.specific.SpecificRecord
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.checkerframework.checker.units.qual.s

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

  @transient
  lazy val encoderWriterPairOpt
      : Option[(JsonEncoder, GenericDatumWriter[GenericRecord])] = {
    if (
      schemaOpt.nonEmpty || classOf[SpecificRecord].isAssignableFrom(
        avroClass
      )
    ) {
      val schema = schemaOpt.getOrElse(
        avroClass.getConstructor().newInstance().getSchema
      )
      Some(
        EncoderFactory
          .get()
          .jsonEncoder(schema, new ByteArrayOutputStream()),
        new GenericDatumWriter[GenericRecord](schema)
      )
    } else None
  }

  lazy val lineEndBytes: Array[Byte] =
    System.lineSeparator().getBytes(StandardCharsets.UTF_8)

  override def encode(element: E, stream: OutputStream): Unit = {
    val record = element.$record

    val (encoder, writer) =
      encoderWriterPairOpt match {
        case None =>
          val schema = record.getSchema
          (
            EncoderFactory.get().jsonEncoder(schema, stream),
            new GenericDatumWriter[GenericRecord](schema)
          )
        case Some(
              (enc: JsonEncoder, wr: GenericDatumWriter[GenericRecord])
            ) =>
          (enc.configure(stream), wr)
      }

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
