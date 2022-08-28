package io.epiphanous.flinkrunner.model

import enumeratum.{Enum, EnumEntry}

import scala.collection.immutable

sealed trait QuotingEscapeStrategy extends EnumEntry

object QuotingEscapeStrategy extends Enum[QuotingEscapeStrategy] {
  override def values: immutable.IndexedSeq[QuotingEscapeStrategy] =
    findValues

  case object EscapeChar extends QuotingEscapeStrategy
  case object Doubling   extends QuotingEscapeStrategy
  case object SqlServer  extends QuotingEscapeStrategy
}
