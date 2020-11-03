package io.epiphanous.flinkrunner.model.aggregate
import java.time.Instant

import squants.Quantity

final case class ExponentialMovingAverage(
  dimension: String,
  unit: String,
  value: Double = 0d,
  count: BigInt = BigInt(0),
  aggregatedLastUpdated: Instant = Instant.EPOCH,
  lastUpdated: Instant = Instant.now(),
  dependentAggregations: Map[String, Aggregate] = Map.empty[String, Aggregate],
  params: Map[String, String] = Map("alpha" -> ExponentialMovingAverage.defaultAlpha))
    extends Aggregate {

  def alpha: Double = params.getOrElse("alpha", ExponentialMovingAverage.defaultAlpha).toDouble

  override def updateQuantity[A <: Quantity[A]](current: A, quantity: A, depAggs: Map[String, Aggregate]) =
    if (count == 0) quantity else current * (1 - alpha) + quantity * alpha

}

object ExponentialMovingAverage {
  final val DEFAULT_ALPHA = 0.7
  def defaultAlpha = DEFAULT_ALPHA.toString
  def apply(dimension: String, unit: String, alpha: Double): ExponentialMovingAverage =
    ExponentialMovingAverage(dimension, unit, params = Map("alpha" -> alpha.toString))
}
