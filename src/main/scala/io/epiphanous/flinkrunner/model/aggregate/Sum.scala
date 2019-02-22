package io.epiphanous.flinkrunner.model.aggregate
import java.time.Instant

import squants.{Quantity, UnitOfMeasure}

final case class Sum[A <: Quantity[A]](
  unit: UnitOfMeasure[A],
  value: Option[A] = None,
  count: BigInt = BigInt(0),
  name: String = "Sum",
  aggregatedLastUpdated: Instant = Instant.now(),
  lastUpdated: Instant = Instant.now())
    extends Aggregate[A] {
  override def update(q: A, aggLastUpdated: Instant) =
    copy(value = Some(value.map(_ + q).getOrElse(q)), count = count + 1, aggregatedLastUpdated = aggLastUpdated)
}
