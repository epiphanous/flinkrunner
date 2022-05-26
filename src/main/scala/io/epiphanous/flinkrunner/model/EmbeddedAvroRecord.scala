package io.epiphanous.flinkrunner.model

import org.apache.avro.generic.GenericContainer

trait EmbeddedAvroRecord {
  def $recordKey: Option[String]
  def $record: GenericContainer
}
