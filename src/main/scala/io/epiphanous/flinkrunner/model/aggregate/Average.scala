package io.epiphanous.flinkrunner.model.aggregate
import java.time.Instant

import squants.{Quantity, UnitOfMeasure}

final case class Average[A <: Quantity[A]](
  unit: UnitOfMeasure[A],
  value: Option[A] = None,
  count: BigInt = BigInt(0),
  name: String = "Average",
  aggregatedLastUpdated: Instant = Instant.now(),
  lastUpdated: Instant = Instant.now(),
  sumOpt: Option[Sum[A]] = None)
    extends Aggregate[A] {

  def sum = sumOpt.getOrElse(Sum(unit))

  override def update(q: A, aggLastUpdated: Instant) = {
    val updatedSum = sum.update(q, aggLastUpdated)
    val updatedCount = count + 1
    copy(value = Some(updatedSum.getValue.divide(updatedCount.doubleValue())),
         count = updatedCount,
         sumOpt = Some(updatedSum),
         aggregatedLastUpdated = aggLastUpdated)
  }
}
