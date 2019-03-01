package io.epiphanous.flinkrunner.model.aggregate
import java.time.Instant

import squants.Quantity

final case class SumOfSquaredDeviations(
  dimension: String,
  unit: String,
  value: Double = 0d,
  name: String = "Sum",
  count: BigInt = BigInt(0),
  aggregatedLastUpdated: Instant = Instant.EPOCH,
  lastUpdated: Instant = Instant.now(),
  dependentAggregations: Map[String, Aggregate] = Map.empty[String, Aggregate],
  params: Map[String, Any] = Map.empty[String, Any])
    extends Aggregate {

  override def getDependents = {
    if (this.dependentAggregations.isEmpty)
      Map("Mean" -> Mean(dimension, unit))
    else this.dependentAggregations
  }

  // see https://www.johndcook.com/blog/standard_deviation/
  override def updateQuantity[A <: Quantity[A]](current: A, quantity: A, depAggs: Map[String, Aggregate]) = {
    val q = quantity in current.unit
    val currentMean = q.unit(getDependents("Mean").value)
    val updatedMean = q.unit(depAggs("Mean").value)
    current + (q - currentMean) * (q - updatedMean).value
  }
}
