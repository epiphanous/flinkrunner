package io.epiphanous.flinkrunner.model.aggregate

import java.time.Instant

import squants.Quantity

final case class StandardDeviation(
  dimension: String,
  unit: String,
  value: Double = 0d,
  name: String = "StandardDeviation",
  count: BigInt = BigInt(0),
  aggregatedLastUpdated: Instant = Instant.EPOCH,
  lastUpdated: Instant = Instant.now(),
  dependentAggregations: Map[String, Aggregate] = Map.empty[String, Aggregate],
  params: Map[String, Any] = Map.empty[String, Any])
    extends Aggregate {

  override def updateQuantity[A <: Quantity[A]](current: A, quantity: A, depAggs: Map[String, Aggregate]) = {
    val updatedVariance = depAggs("Variance")
    current.unit(Math.sqrt(updatedVariance.value))
  }

}

object StandardDeviation {
  def apply(dimension: String, unit: String): StandardDeviation =
    StandardDeviation(dimension, unit, dependentAggregations = Map("Variance" -> Variance(dimension, unit)))
}
