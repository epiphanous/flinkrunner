package io.epiphanous.flinkrunner.avro

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class TestAvroSchemaRegistryClient extends AvroSchemaRegistryClient {
  val schemas = mutable.Map.empty[Either[Int, String], RegisteredAvroSchema]

  def install(schema: RegisteredAvroSchema): Unit = {
    schemas.update(Left(schema.id), schema)
    schemas.update(Right(schema.subject), schema)
  }

  def clear(): Unit =
    schemas.clear()

  override def get(id: Int): Try[RegisteredAvroSchema] =
    schemas.get(Left(id)).map(Success(_)).getOrElse(Failure(new Throwable(s"schema with id $id not found")))

  override def get(name: String): Try[RegisteredAvroSchema] =
    schemas.get(Right(name)).map(Success(_)).getOrElse(Failure(new Throwable(s"schema with subject $name not found")))

  override def get[E](event: E, isKey: Boolean = false): Try[RegisteredAvroSchema] = get(subject(event, isKey))

  override def subject[E](event: E, isKey: Boolean): String =
    Array(event.getClass.getCanonicalName, if (isKey) "key" else "value").mkString("-")
}
