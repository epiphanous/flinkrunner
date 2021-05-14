package io.epiphanous.flinkrunner.avro

import java.nio.ByteBuffer
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class TestSchemaRegistryClient
    extends AvroSchemaRegistryClient[ConfluentSchemaRegistryContext] {
  val schemas =
    mutable.Map.empty[String, RegisteredAvroSchema]

  def install(schema: RegisteredAvroSchema): RegisteredAvroSchema = {
    schemas.update(schema.id, schema)
    schemas.update(schema.subject, schema)
    schema
  }

  def clear(): Unit =
    schemas.clear()

  override def getFromMessage(
      message: Array[Byte],
      optContext: Option[ConfluentSchemaRegistryContext])
      : Try[(RegisteredAvroSchema, Array[Byte])] =
    for {
      bb <- Try(ByteBuffer.wrap(message))
      id <- if (bb.get == 0x0) Try(bb.getInt().toString)
            else Failure(new Throwable("failed to get magic from message"))
      schema <- if (schemas.contains(id)) Success(schemas(id))
                else Failure(new Throwable(s"can't find schema $id"))
      bytes <- Try(message.slice(5, message.length))
    } yield (schema, bytes)

  override def getFromEvent[E](
      event: E,
      optContext: Option[ConfluentSchemaRegistryContext])
      : Try[(RegisteredAvroSchema, Array[Byte])] = {
    val subject = getSubjectName(event, optContext)
    schemas.get(subject) match {
      case Some(schema) =>
        Success(
          (
            schema,
            ByteBuffer
              .allocate(5)
              .put(0x0.toByte)
              .putInt(schema.id.toInt)
              .array()
          )
        )
      case _            => Failure(new Throwable(s"schema $subject not found"))
    }
  }

  override def put(
      schema: RegisteredAvroSchema,
      optContext: Option[ConfluentSchemaRegistryContext])
      : Try[RegisteredAvroSchema] =
    Try(install(schema))

  def get(
      key: String,
      optContext: Option[ConfluentSchemaRegistryContext] = None)
      : Try[RegisteredAvroSchema] = if (schemas.contains(key))
    Success(schemas(key))
  else Failure(new Throwable(s"missing schema $key"))

  def getSubjectName[E](
      event: E,
      optContext: Option[ConfluentSchemaRegistryContext] = None): String =
    Array(
      event.getClass.getCanonicalName,
      if (optContext.getOrElse(ConfluentSchemaRegistryContext()).isKey)
        "key"
      else "value"
    )
      .mkString("-")
}
