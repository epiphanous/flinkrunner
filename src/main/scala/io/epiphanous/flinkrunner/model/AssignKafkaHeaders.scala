package io.epiphanous.flinkrunner.model

import org.apache.avro.generic.GenericRecord

trait AssignKafkaHeaders[A <: GenericRecord] {
  this: FlinkEvent with EmbeddedAvroRecord[A] =>

  def assignHeaders: Seq[KafkaHeaderMapper] =
    KafkaInfoHeader.values.map(KafkaHeaderMapper.apply)

  def assignKafkaHeaders(): Unit =
    assignHeaders.foreach(_.assign($record, $recordHeaders))

}
