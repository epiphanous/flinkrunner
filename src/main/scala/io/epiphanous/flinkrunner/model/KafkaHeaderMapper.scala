package io.epiphanous.flinkrunner.model

import KafkaInfoHeader.{headerFieldName, headerName, headerValueMapper}
import org.apache.avro.generic.GenericRecord

case class KafkaHeaderMapper(
    headerName: String,
    recordField: String,
    mapper: String => Option[Any]) {
  def assign[A <: GenericRecord](
      record: A,
      headers: Map[String, String]): Unit =
    if (record.hasField(recordField))
      headers
        .get(headerName)
        .foreach(headerValue =>
          record.put(recordField, mapper(headerValue).orNull)
        )
}

object KafkaHeaderMapper {
  def apply(header: KafkaInfoHeader): KafkaHeaderMapper =
    KafkaHeaderMapper(
      headerName(header),
      headerFieldName(header),
      headerValueMapper(header)
    )
}
