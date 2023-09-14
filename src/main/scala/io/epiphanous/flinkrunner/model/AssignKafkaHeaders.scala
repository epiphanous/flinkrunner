package io.epiphanous.flinkrunner.model

import org.apache.avro.generic.GenericRecord

trait AssignKafkaHeaders[A <: GenericRecord] {
  this: {
    val $record: A
    val $recordHeaders: Map[String, String]
  } =>

  val defaultAssignableHeaders: Seq[KafkaHeaderMapper] =
    KafkaInfoHeader.values.map(KafkaHeaderMapper.apply)

  def customAssignableHeaders: Seq[KafkaHeaderMapper] = Seq.empty

  def assignKafkaHeaders(): Unit =
    (defaultAssignableHeaders ++ customAssignableHeaders).foreach(
      _.assign($record, $recordHeaders)
    )

  assignKafkaHeaders()

}
