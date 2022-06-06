package io.epiphanous.flinkrunner.model.aggregate
import enumeratum.EnumEntry.{Lowercase, Snakecase}
import enumeratum._

import scala.collection.immutable
sealed trait AggregateType extends EnumEntry with Snakecase with Lowercase

object AggregateType extends Enum[AggregateType] {
  override val values: immutable.IndexedSeq[AggregateType] = findValues

  case object Count                              extends AggregateType
  case object ExponentialMovingAverage           extends AggregateType
  case object ExponentialMovingStandardDeviation extends AggregateType
  case object ExponentialMovingVariance          extends AggregateType
  case object Histogram                          extends AggregateType
  case object Max                                extends AggregateType
  case object Mean                               extends AggregateType
  case object Min                                extends AggregateType
  case object Percentage                         extends AggregateType
  case object Range                              extends AggregateType
  case object StandardDeviation                  extends AggregateType
  case object Sum                                extends AggregateType
  case object SumOfSquaredDeviations             extends AggregateType
  case object Variance                           extends AggregateType
}
