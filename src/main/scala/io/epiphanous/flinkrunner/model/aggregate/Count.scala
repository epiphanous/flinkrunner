package io.epiphanous.flinkrunner.model.aggregate
import java.time.Instant

import io.epiphanous.flinkrunner.model.UnitMapper
import squants.{Dimensionless, Each, Quantity}

final case class Count(
  dimension: String,
  unit: String,
  value: Double = 0d,
  count: BigInt = BigInt(0),
  aggregatedLastUpdated: Instant = Instant.EPOCH,
  lastUpdated: Instant = Instant.now(),
  dependentAggregations: Map[String, Aggregate] = Map.empty[String, Aggregate],
  params: Map[String, String] = Map.empty[String, String])
    extends Aggregate {

  override def isDimensionless = true

  override def outUnit: String = Each.symbol

  override def updateQuantity[A <: Quantity[A]](current: A, quantity: A, depAggs: Map[String, Aggregate]) =
    current + current.unit(1)

}

object Count {
  def apply(): Count = new Count(Dimensionless.name, Each.symbol)
}
