package io.epiphanous.flinkrunner.avro

import scala.util.Try

trait AvroSchemaRegistryClient {
  def get(id: Int): Try[RegisteredAvroSchema]

  def get(name: String): Try[RegisteredAvroSchema]

  def get[E](event: E, isKey: Boolean = false): Try[RegisteredAvroSchema]

  def subject[E](event: E, isKey: Boolean = false): String
}
