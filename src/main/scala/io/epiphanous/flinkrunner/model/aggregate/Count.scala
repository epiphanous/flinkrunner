package io.epiphanous.flinkrunner.model.aggregate

import squants.{Dimensionless, Each, Quantity}

import java.time.Instant

final case class Count(
    dimension: String,
    unit: String,
    value: Double = 0d,
    count: BigInt = BigInt(0),
    aggregatedLastUpdated: Instant = Instant.EPOCH,
    lastUpdated: Instant = Instant.now(),
    dependentAggregations: Map[String, Aggregate] =
      Map.empty[String, Aggregate],
    params: Map[String, String] = Map.empty[String, String])
    extends Aggregate {

  override def isDimensionless = true

  override def outUnit: String = Each.symbol

  override def updateQuantity[A <: Quantity[A]](
      current: A,
      quantity: A,
      depAggs: Map[String, Aggregate]): A =
    current + current.unit(1)

}

object Count {
  def apply(): Count = new Count(Dimensionless.name, Each.symbol)
}
