package io.epiphanous.flinkrunner.avro

import com.sksamuel.avro4s.{Decoder, Encoder}
import com.typesafe.scalalogging.LazyLogging

import scala.util.Try

/**
 * Uses a provided schema registry client to facilitate avro encoding and
 * decoding.
 *
 * @param registry
 *   the schema registry client
 * @tparam Context
 *   the schema registry context type
 */
class AvroCoder[Context](registry: AvroSchemaRegistryClient[Context])
    extends LazyLogging {

  /**
   * Decode a given binary avro encoded byte array using a schema
   * identified by some prefix at the start of the encoded bytes, and
   * optionally additional context information.
   *
   * @param message
   *   the raw byte array
   * @param optContext
   *   any additional context, besides a message prefix, needed to look up
   *   the avro schema
   * @tparam E
   *   the expected type of payload
   * @return
   *   An instance of E (wrapped in a [[Try]] )
   */
  def decode[E: Decoder](
      message: Array[Byte],
      optContext: Option[Context] = None): Try[E] = for {
    (schema, bytes) <- registry.getFromMessage(message, optContext)
    event <- schema.decode[E](bytes)
  } yield event

  /**
   * Avro encode an event record, using the latest schema found in the
   * registry under the event record's class name (as returned by
   * event.getClass.getSimpleName).
   *
   * @param event
   *   E An event record
   * @tparam E
   *   class of the event record
   * @return
   *   A byte array (wrapped in a [[Try]] )
   */
  def encode[E: Encoder](
      event: E,
      optContext: Option[Context] = None): Try[Array[Byte]] =
    registry
      .getFromEvent(event, optContext)
      .flatMap { case (schema, magic) => schema.encode(event, magic) }
}
