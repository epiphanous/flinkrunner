package io.epiphanous.flinkrunner.model.aggregate
import java.time.Instant

import io.epiphanous.flinkrunner.model.UnitMapper
import squants.Each

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

  override def update(
    value: Double,
    unit: String,
    aggLU: Instant,
    unitMapper: UnitMapper = UnitMapper.defaultUnitMapper
  ) =
    Some(copy(value = this.value + 1, unit = outUnit, count = count + 1, aggregatedLastUpdated = aggLU))
}
