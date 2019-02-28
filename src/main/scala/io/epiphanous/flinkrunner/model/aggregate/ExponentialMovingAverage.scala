package io.epiphanous.flinkrunner.model.aggregate
import java.time.Instant

import squants.{Quantity, UnitOfMeasure}

final case class ExponentialMovingAverage(
  dimension: String,
  unit: String,
  value: Double = 0d,
  name: String = "ExponentialMovingAverage",
  count: BigInt = BigInt(0),
  aggregatedLastUpdated: Instant = Instant.EPOCH,
  lastUpdated: Instant = Instant.now(),
  dependentAggregations: Map[String, Aggregate] = Map.empty[String, Aggregate],
  params: Map[String, Any] = Map("alpha" -> 0.7))
    extends Aggregate {

  def alpha: Double = params.getOrElse("alpha", 0.7).asInstanceOf[Double]

  def withAlpha(alpha: Double): ExponentialMovingAverage = copy(params = Map("alpha" -> alpha))

  override def updateQuantity[A <: Quantity[A]](current: A, quantity: A, depAggs: Map[String, Aggregate]) =
    current * (1 - alpha) + quantity * alpha

}

object ExponentialMovingAverage {
  def apply(dimension: String, unit: String, alpha: Double): ExponentialMovingAverage =
    ExponentialMovingAverage(dimension, unit).withAlpha(alpha)
}
