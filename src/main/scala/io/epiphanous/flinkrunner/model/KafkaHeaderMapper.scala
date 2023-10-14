package io.epiphanous.flinkrunner.model

import KafkaInfoHeader.{headerFieldName, headerName, headerValueMapper}
import org.apache.avro.generic.GenericRecord

import java.time.Instant
import scala.util.Try

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

  def intMapper(value: String): Option[Int]       = Try(value.toInt).toOption
  def stringMapper(value: String): Option[String] = Option(value)

  def longMapper(value: String): Option[Long]       = Try(value.toLong).toOption
  def booleanMapper(value: String): Option[Boolean] = Try(
    value.toBoolean
  ).toOption

}
