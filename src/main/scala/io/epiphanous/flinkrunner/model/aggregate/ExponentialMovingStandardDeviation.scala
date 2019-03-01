package io.epiphanous.flinkrunner.model.aggregate
import java.time.Instant

import io.epiphanous.flinkrunner.model.UnitMapper
import squants.Quantity

final case class ExponentialMovingStandardDeviation(
  dimension: String,
  unit: String,
  value: Double = 0d,
  name: String = "ExponentialMovingStandardDeviation",
  count: BigInt = BigInt(0),
  aggregatedLastUpdated: Instant = Instant.EPOCH,
  lastUpdated: Instant = Instant.now(),
  dependentAggregations: Map[String, Aggregate] = Map.empty[String, Aggregate],
  params: Map[String, Any] = Map.empty[String, Any])
    extends Aggregate {

  override def getDependents = {
    if (this.dependentAggregations.isEmpty)
      Map("ExponentialMovingVariance" -> ExponentialMovingVariance(dimension, unit, params = params))
    else this.dependentAggregations
  }

  override def updateQuantity[A <: Quantity[A]](current: A, quantity: A, depAggs: Map[String, Aggregate]) = {
    val updatedEmv = depAggs("ExponentialMovingVariance")
    current.unit(Math.sqrt(updatedEmv.value))
  }

}

object ExponentialMovingStandardDeviation {
  def apply(dimension: String, unit: String, alpha: Double): ExponentialMovingStandardDeviation =
    ExponentialMovingStandardDeviation(
      dimension,
      unit,
      dependentAggregations = Map("ExponentialMovingVariance" -> ExponentialMovingVariance(dimension, unit, alpha)),
      params = Map("alpha" -> alpha)
    )
}
