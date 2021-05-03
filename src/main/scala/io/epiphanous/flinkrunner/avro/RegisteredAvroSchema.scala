package io.epiphanous.flinkrunner.avro

import com.sksamuel.avro4s._
import org.apache.avro.{LogicalTypes, Schema}

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.util.Try

case class RegisteredAvroSchema(
    id: Int,
    schema: Schema,
    subject: String = "",
    version: Int = 0) {

  def name: String = if (subject.isEmpty) schema.getFullName else subject

  def decode[E: Decoder](buffer: ByteBuffer): Try[E] = {
    val bytes = new Array[Byte](buffer.remaining())
    buffer.get(bytes)
    Try(
      AvroInputStream
        .binary[E]
        .from(bytes)
        .build(schema)
        .iterator
        .next()
    )
  }

  def encode[E: Encoder](
      event: E,
      addMagic: Boolean = true): Try[Array[Byte]] =
    Try {
      val baos  = new ByteArrayOutputStream()
      val os    = AvroOutputStream.binary[E].to(baos).build()
      os.write(event)
      os.flush()
      os.close()
      val bytes = baos.toByteArray
      if (addMagic)
        ByteBuffer
          .allocate(bytes.length + 5)
          .put(RegisteredAvroSchema.MAGIC)
          .putInt(id)
          .put(bytes)
          .array()
      else bytes
    }
}

object RegisteredAvroSchema {
  final val MAGIC = 0x0.toByte

  // force java instants to encode/decode with microsecond precision
  implicit val instantSchemaFor: AnyRef with SchemaFor[Instant] =
    SchemaFor[Instant](
      LogicalTypes
        .localTimestampMicros()
        .addToSchema(Schema.create(Schema.Type.LONG))
    )

  implicit val InstantEncoder: Encoder[Instant] =
    Encoder.LongEncoder
      .comap[Instant](instant =>
        ChronoUnit.MICROS.between(Instant.EPOCH, instant)
      )
      .withSchema(instantSchemaFor)

  implicit val InstantDecoder: Decoder[Instant] = Decoder.LongDecoder
    .map[Instant](micros => Instant.EPOCH.plus(micros, ChronoUnit.MICROS))
    .withSchema(instantSchemaFor)

  def schemaFor[E](implicit s: SchemaFor[E]): Schema = AvroSchema[E]
}
