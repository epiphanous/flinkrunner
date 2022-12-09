package io.epiphanous.flinkrunner.model

import enumeratum._

import scala.collection.immutable

sealed trait KafkaInfoHeader extends EnumEntry

object KafkaInfoHeader extends Enum[KafkaInfoHeader] {

  case object SerializedValueSize extends KafkaInfoHeader
  case object SerializedKeySize   extends KafkaInfoHeader
  case object Offset              extends KafkaInfoHeader
  case object Partition           extends KafkaInfoHeader
  case object Timestamp           extends KafkaInfoHeader
  case object TimestampType       extends KafkaInfoHeader
  case object Topic               extends KafkaInfoHeader

  def headerName(h: KafkaInfoHeader) = s"Kafka.${h.entryName}"

  override def values: immutable.IndexedSeq[KafkaInfoHeader] = findValues
}
