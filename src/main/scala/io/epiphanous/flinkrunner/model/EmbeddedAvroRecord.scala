package io.epiphanous.flinkrunner.model

import org.apache.avro.generic.GenericRecord

trait EmbeddedAvroRecord[A <: GenericRecord] {
  def $recordKey: Option[String]
  def $record: A
  def toKV: (Option[String], A) = ($recordKey, $record)
}
