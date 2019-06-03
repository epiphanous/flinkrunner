package io.epiphanous.flinkrunner.model.aggregate
import java.time.Instant

import squants.Quantity

final case class Variance(
  dimension: String,
  unit: String,
  value: Double = 0d,
  count: BigInt = BigInt(0),
  aggregatedLastUpdated: Instant = Instant.EPOCH,
  lastUpdated: Instant = Instant.now(),
  dependentAggregations: Map[String, Aggregate] = Map.empty[String, Aggregate],
  params: Map[String, String] = Map.empty[String, String])
    extends Aggregate {

  override def getDependents = {
    if (this.dependentAggregations.isEmpty)
      Map("SumOfSquaredDeviations" -> SumOfSquaredDeviations(dimension, unit))
    else this.dependentAggregations
  }

  override def updateQuantity[A <: Quantity[A]](current: A, quantity: A, depAggs: Map[String, Aggregate]) = {
    val k = count.doubleValue()
    val s = current.unit(depAggs("SumOfSquaredDeviations").value)
    s / (k - 1)
  }
}
