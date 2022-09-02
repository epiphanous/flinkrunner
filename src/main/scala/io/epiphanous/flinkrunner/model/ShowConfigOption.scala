package io.epiphanous.flinkrunner.model

import enumeratum.EnumEntry.Lowercase
import enumeratum._

import scala.collection.immutable

sealed trait ShowConfigOption extends EnumEntry with Lowercase

object ShowConfigOption extends Enum[ShowConfigOption] {
  case object None      extends ShowConfigOption
  case object Concise   extends ShowConfigOption
  case object Formatted extends ShowConfigOption

  override def values: immutable.IndexedSeq[ShowConfigOption] = findValues
}
