package io.epiphanous.flinkrunner.model.aggregate
import java.time.Instant

import squants.Quantity

final case class Range(
  dimension: String,
  unit: String,
  value: Double = 0d,
  name: String = "Range",
  count: BigInt = BigInt(0),
  aggregatedLastUpdated: Instant = Instant.EPOCH,
  lastUpdated: Instant = Instant.now(),
  dependentAggregations: Map[String, Aggregate] = Map.empty[String, Aggregate],
  params: Map[String, Any] = Map.empty[String, Any])
    extends Aggregate {

  override def updateQuantity[A <: Quantity[A]](current: A, quantity: A, depAggs: Map[String, Aggregate]) =
    current.unit(depAggs("Max").value) - current.unit(depAggs("Min").value)
}

object Range {
  def apply(dimension: String, unit: String): Range =
    Range(dimension, unit, dependentAggregations = Map("Min" -> Min(dimension, unit), "Max" -> Max(dimension, unit)))
}
