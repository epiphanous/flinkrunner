package io.epiphanous.flinkrunner.model.aggregate
import java.time.Instant

import squants.{Quantity, UnitOfMeasure}

final case class ExponentialMovingAverage[A <: Quantity[A]](
  unit: UnitOfMeasure[A],
  value: Option[A] = None,
  count: BigInt = BigInt(0),
  name: String = "EMA",
  aggregatedLastUpdated: Instant = Instant.now(),
  lastUpdated: Instant = Instant.now(),
  alpha: Double = 0.7)
    extends Aggregate[A] {
  override def update(q: A, aggLastUpdated: Instant) =
    copy(value = Some(value.map(ma => (1 - alpha) * ma + alpha * q).getOrElse(q)),
         count = count + 1,
         aggregatedLastUpdated = aggLastUpdated)
}
