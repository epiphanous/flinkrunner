package io.epiphanous.flinkrunner.model

import enumeratum.EnumEntry.Hyphencase
import enumeratum.{Enum, EnumEntry}

sealed trait TableFormat extends EnumEntry with Hyphencase

object TableFormat extends Enum[TableFormat] {
  val values = findValues

  case object CSV           extends TableFormat
  case object JSON          extends TableFormat
  case object Avro          extends TableFormat
  case object AvroConfluent extends TableFormat
  case object Debezium      extends TableFormat
  case object Canal         extends TableFormat
  case object Maxwell       extends TableFormat
  case object Parquet       extends TableFormat
  case object Orc           extends TableFormat
  case object Raw           extends TableFormat

}
