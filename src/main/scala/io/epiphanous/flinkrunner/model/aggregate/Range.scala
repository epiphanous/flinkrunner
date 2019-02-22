package io.epiphanous.flinkrunner.model.aggregate
import java.time.Instant

import squants.{Quantity, UnitOfMeasure}

final case class Range[A <: Quantity[A]](
  unit: UnitOfMeasure[A],
  value: Option[A] = None,
  count: BigInt = BigInt(0),
  name: String = "Range",
  aggregatedLastUpdated: Instant = Instant.now(),
  lastUpdated: Instant = Instant.now(),
  minOpt: Option[Min[A]] = None,
  maxOpt: Option[Max[A]] = None)
    extends Aggregate[A] {

  def min = minOpt.getOrElse(Min(unit))

  def max = maxOpt.getOrElse(Max(unit))

  override def update(q: A, aggLastUpdated: Instant) = {
    val updatedMin = min.update(q, aggLastUpdated)
    val updatedMax = max.update(q, aggLastUpdated)
    copy(value = Some(updatedMax.getValue - updatedMin.getValue),
         count = count + 1,
         aggregatedLastUpdated = aggLastUpdated,
         minOpt = Some(updatedMin),
         maxOpt = Some(updatedMax))
  }
}
