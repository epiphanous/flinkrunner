package io.epiphanous.flinkrunner.model

import enumeratum.EnumEntry.Snakecase
import enumeratum._
import io.epiphanous.flinkrunner.util.RichString.RichString

import java.time.Instant
import scala.collection.immutable
import scala.util.Try

sealed trait KafkaInfoHeader extends EnumEntry with Snakecase

object KafkaInfoHeader extends Enum[KafkaInfoHeader] {

  case object SerializedValueSize extends KafkaInfoHeader
  case object SerializedKeySize   extends KafkaInfoHeader
  case object Offset              extends KafkaInfoHeader
  case object Partition           extends KafkaInfoHeader
  case object Timestamp           extends KafkaInfoHeader
  case object TimestampType       extends KafkaInfoHeader
  case object Topic               extends KafkaInfoHeader

  def headerName(h: KafkaInfoHeader) = s"Kafka.${h.entryName}"

  def headerFieldName(h: KafkaInfoHeader) =
    s"kafka_${h.entryName.snakeCase}"

  def headerValueMapper(h: KafkaInfoHeader): String => Option[Any] = {
    headerValue: String =>
      Try(h match {
        case SerializedValueSize | SerializedKeySize | Partition =>
          headerValue.toInt
        case Offset                                              => headerValue.toLong
        case Timestamp                                           => Instant.ofEpochMilli(headerValue.toLong)
        case _                                                   => headerValue
      }).toOption
  }

  override def values: immutable.IndexedSeq[KafkaInfoHeader] = findValues

}
