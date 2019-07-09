package io.epiphanous.flinkrunner.avro

import scala.collection.mutable
import scala.util.{Failure, Success}

class TestAvroSchemaRegistryClient extends AvroSchemaRegistryClient {
  val schemas = mutable.Map.empty[Either[Int, String], RegisteredAvroSchema]

  def install(schema: RegisteredAvroSchema) = {
    schemas.update(Left(schema.id), schema)
    schemas.update(Right(schema.subject), schema)
  }

  def clear(): Unit =
    schemas.clear()

  override def get(id: Int) =
    schemas.get(Left(id)).map(Success(_)).getOrElse(Failure(new Throwable(s"schema with id $id not found")))

  override def get(name: String) =
    schemas.get(Right(name)).map(Success(_)).getOrElse(Failure(new Throwable(s"schema with subject $name not found")))

  override def get[E](event: E, isKey: Boolean = false) = get(subject(event, isKey))

  override def subject[E](event: E, isKey: Boolean) =
    Array(event.getClass.getCanonicalName, if (isKey) "key" else "value").mkString("-")
}
