package io.epiphanous.flinkrunner.model.aggregate

import squants.Quantity

import java.time.Instant

final case class Min(
                      dimension: String,
                      unit: String,
                      value: Double = Double.MaxValue,
                      count: BigInt = BigInt(0),
                      aggregatedLastUpdated: Instant = Instant.EPOCH,
                      lastUpdated: Instant = Instant.now(),
                      dependentAggregations: Map[String, Aggregate] = Map.empty[String, Aggregate],
                      params: Map[String, String] = Map.empty[String, String])
  extends Aggregate {

  override def updateQuantity[A <: Quantity[A]](current: A, quantity: A, depAggs: Map[String, Aggregate]) =
    if (count == 0) quantity else current.min(quantity)

}
