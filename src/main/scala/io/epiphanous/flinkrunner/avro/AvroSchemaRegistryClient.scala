package io.epiphanous.flinkrunner.avro

import scala.util.Try

/**
 * A trait for an avro schema registry
 */
@deprecated(
  "Use the ConfluentAvroRegistryKafkaRecordSerialization and Deserialization classes instead",
  "4.0.0"
)
trait AvroSchemaRegistryClient[Context] {

  /**
   * Most schema registries encode some special magic bytes and identifying
   * schema information as a prefix on the actual encoded event bytes. This
   * method should strip those identifying prefix bytes from the message,
   * find the identified schema, and return the schema and the actual
   * encoded bytes.
   * @param message
   *   a byte array containing any schema identification as well as a
   *   binary avro encoded message
   * @param optContext
   *   the optional context
   * @return
   *   a (schema, bytes) tuple, wrapped in a [[Try]]
   */
  def getFromMessage(
      message: Array[Byte],
      optContext: Option[Context] = None)
      : Try[(RegisteredAvroSchema, Array[Byte])]

  /**
   * Retrieve a schema based on an event of type E, and optionally, some
   * additional context information. This is primarily used when
   * serializing an event. In addition to determining the schema that
   * should be used, this method also is responsible for computing a byte
   * array representing any magic that should be prepended to the
   * serialized event.
   * @param event
   *   the event
   * @param optContext
   *   the optional context
   * @tparam E
   *   the type of event
   * @return
   *   a (schema, magic) tuple, wrapped in a [[Try]]
   */
  def getFromEvent[E](event: E, optContext: Option[Context] = None)
      : Try[(RegisteredAvroSchema, Array[Byte])]

  /**
   * Retrieve a schema based on its id or subject, and optionally, some
   * additional context information.
   * @param idOrSubject
   *   the id or subject key
   * @param optContext
   *   the optional context
   * @return
   */
  def get(
      idOrSubject: String,
      optContext: Option[Context] = None): Try[RegisteredAvroSchema]

  /**
   * Install a schema into the registry under its id and subject keys.
   * @param schema
   *   the schema
   * @param optContext
   *   optional context
   * @return
   *   the schema, wrapped in a [[Try]]
   */
  def put(
      schema: RegisteredAvroSchema,
      optContext: Option[Context] = None): Try[RegisteredAvroSchema]
}
