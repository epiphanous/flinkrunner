package io.epiphanous.flinkrunner.model.aggregate
import java.time.Instant

import squants.Quantity

final case class Max(
  dimension: String,
  unit: String,
  value: Double = 0d,
  count: BigInt = BigInt(0),
  aggregatedLastUpdated: Instant = Instant.EPOCH,
  lastUpdated: Instant = Instant.now(),
  dependentAggregations: Map[String, Aggregate] = Map.empty[String, Aggregate],
  params: Map[String, String] = Map.empty[String, String])
    extends Aggregate {

  override def updateQuantity[A <: Quantity[A]](current: A, quantity: A, depAggs: Map[String, Aggregate]) =
    if (count == 0) quantity else current.max(quantity)

}
