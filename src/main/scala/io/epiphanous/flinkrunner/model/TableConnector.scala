package io.epiphanous.flinkrunner.model

import enumeratum.EnumEntry.Hyphencase
import enumeratum.{Enum, EnumEntry}

sealed trait TableConnector extends EnumEntry with Hyphencase

object TableConnector extends Enum[TableConnector] {
  val values = findValues

  case object Kafka         extends TableConnector
  case object UpsertKafka   extends TableConnector
  case object Kinesis       extends TableConnector
  case object JDBC          extends TableConnector
  case object Elasticsearch extends TableConnector
  case object FileSystem    extends TableConnector
  case object HBase         extends TableConnector
  case object DataGen       extends TableConnector
  case object Print         extends TableConnector
  case object BlackHole     extends TableConnector
  case object Hive          extends TableConnector

}
