package io.epiphanous.flinkrunner.model

import enumeratum.EnumEntry.{Lowercase, Uppercase}
import enumeratum._

import scala.collection.immutable

sealed trait StreamFormatName
    extends EnumEntry
    with Lowercase
    with Uppercase

object StreamFormatName extends Enum[StreamFormatName] {
  override def values: immutable.IndexedSeq[StreamFormatName] =
    findValues

  case object Json extends StreamFormatName

  case object Csv extends StreamFormatName

  case object Tsv extends StreamFormatName

  case object Psv extends StreamFormatName

  case object Delimited extends StreamFormatName

  case object Parquet extends StreamFormatName

  def isBulk(format: StreamFormatName): Boolean = format match {
    case Parquet => true
    case _       => false
  }

}