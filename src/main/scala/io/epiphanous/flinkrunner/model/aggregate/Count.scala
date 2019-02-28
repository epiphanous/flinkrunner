package io.epiphanous.flinkrunner.model.aggregate
import java.time.Instant

import io.epiphanous.flinkrunner.model.UnitMapper

final case class Count(
  dimension: String = "Dimensionless",
  unit: String = "each",
  value: Double = 0d,
  name: String = "Count",
  count: BigInt = BigInt(0),
  aggregatedLastUpdated: Instant = Instant.EPOCH,
  lastUpdated: Instant = Instant.now(),
  dependentAggregations: Map[String, Aggregate] = Map.empty[String, Aggregate],
  params: Map[String, Any] = Map.empty[String, Any])
    extends Aggregate {

  override def update(
    value: Double,
    unit: String,
    aggLU: Instant,
    unitMapper: UnitMapper = UnitMapper.defaultUnitMapper
  ) =
    Some(copy(value = value + 1, aggregatedLastUpdated = aggLU))
}
