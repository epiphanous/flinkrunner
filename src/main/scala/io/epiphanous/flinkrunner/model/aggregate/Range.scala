package io.epiphanous.flinkrunner.model.aggregate

import squants.Quantity

import java.time.Instant

final case class Range(
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
      Map("Min" -> Min(dimension, unit), "Max" -> Max(dimension, unit))
    else this.dependentAggregations
  }

  override def updateQuantity[A <: Quantity[A]](current: A, quantity: A, depAggs: Map[String, Aggregate]) =
    if (count == 0) current else current.unit(depAggs("Max").value - depAggs("Min").value)
}
