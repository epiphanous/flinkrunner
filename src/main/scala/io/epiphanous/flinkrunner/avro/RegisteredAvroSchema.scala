package io.epiphanous.flinkrunner.avro

import com.sksamuel.avro4s._
import org.apache.avro.{LogicalTypes, Schema}

import java.io.ByteArrayOutputStream
import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.util.Try

@deprecated(
  "Use the ConfluentAvroRegistryKafkaRecordSerialization and Deserialization classes instead",
  "4.0.0"
)
case class RegisteredAvroSchema(
    schema: Schema,
    id: String,
    optSubject: Option[String] = None,
    optVersion: Option[String] = None) {

  /** subject (name) of the schema */
  val subject: String = optSubject.getOrElse(schema.getFullName)

  /**
   * Decode an array of bytes into an event of type E.
   * @param bytes
   *   a binary avro encoded array of bytes that can be decoded into type E
   * @tparam E
   *   the type of event to decode
   * @return
   *   the event, wrapped in a [[Try]]
   */
  def decode[E: Decoder](bytes: Array[Byte]): Try[E] = {
    Try(
      AvroInputStream.binary
        .from(bytes)
        .build(schema)
        .iterator
        .next()
    )
  }

  /**
   * Binary avro encode an event of type E, prepending the result with the
   * provided magic byte array.
   * @param event
   *   the event to encode
   * @param magic
   *   an array of bytes to prepend to the result that can be used to
   *   identify the schema to decode the bytes with
   * @tparam E
   *   the type of event
   * @return
   *   an array of bytes, wrapped in a [[Try]]
   */
  def encode[E: Encoder](
      event: E,
      magic: Array[Byte] = Array.emptyByteArray): Try[Array[Byte]] =
    Try {
      val baos = new ByteArrayOutputStream()
      val os   = AvroOutputStream.binary.to(baos).build()
      os.write(event)
      os.flush()
      os.close()
      magic ++ baos.toByteArray
    }
}

object RegisteredAvroSchema {
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
