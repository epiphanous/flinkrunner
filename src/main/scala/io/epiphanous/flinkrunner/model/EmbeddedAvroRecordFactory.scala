package io.epiphanous.flinkrunner.model

import org.apache.avro.generic.GenericRecord

trait EmbeddedAvroRecordFactory[
    E <: FlinkEvent with EmbeddedAvroRecord[A],
    A <: GenericRecord] {
  implicit def fromKV(keyOpt: Option[String], record: A): E
}
