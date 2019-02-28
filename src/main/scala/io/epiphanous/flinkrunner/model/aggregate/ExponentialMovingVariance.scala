package io.epiphanous.flinkrunner.model.aggregate
import java.time.Instant

import squants.Quantity

final case class ExponentialMovingVariance(
  dimension: String,
  unit: String,
  value: Double = 0d,
  name: String = "ExponentialMovingVariance",
  count: BigInt = BigInt(0),
  aggregatedLastUpdated: Instant = Instant.EPOCH,
  lastUpdated: Instant = Instant.now(),
  dependentAggregations: Map[String, Aggregate] = Map.empty[String, Aggregate],
  params: Map[String, Any] = Map("alpha" -> 0.7))
    extends Aggregate {

  def alpha = params.getOrElse("alpha", 0.7).asInstanceOf[Double]

  def withAlpha(alpha: Double): ExponentialMovingVariance = copy(params = Map("alpha" -> alpha))

  override def updateQuantity[A <: Quantity[A]](current: A, quantity: A, depAggs: Map[String, Aggregate]) = {
    val currentEma = this.dependentAggregations("ExponentialMovingAverage")
    val q = quantity in current.unit
    val delta = q - current.unit(currentEma.value)
    (1 - alpha) * (current + delta * delta.value * alpha)
  }

}

object ExponentialMovingVariance {
  def apply(dimension: String, unit: String, alpha: Double): ExponentialMovingVariance =
    ExponentialMovingVariance(
      dimension,
      unit,
      dependentAggregations = Map("ExponentialMovingAverage" -> ExponentialMovingAverage(dimension, unit, alpha))
    ).withAlpha(alpha)

}
