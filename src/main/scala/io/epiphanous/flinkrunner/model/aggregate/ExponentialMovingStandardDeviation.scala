package io.epiphanous.flinkrunner.model.aggregate
import java.time.Instant

import squants.{Quantity, UnitOfMeasure}

final case class ExponentialMovingStandardDeviation[A <: Quantity[A]](
  unit: UnitOfMeasure[A],
  value: Option[A] = None,
  count: BigInt = BigInt(0),
  name: String = "EMSD",
  aggregatedLastUpdated: Instant = Instant.now(),
  lastUpdated: Instant = Instant.now(),
  emvOpt: Option[ExponentialMovingVariance[A]] = None,
  alpha: Double = 0.7)
    extends Aggregate[A] {

  def emv = emvOpt.getOrElse(ExponentialMovingVariance(unit))

  override def update(q: A, aggLastUpdated: Instant) = {
    val updatedEmv = emv.update(q, aggLastUpdated)
    copy(value = Some(unit(Math.sqrt(updatedEmv.getValue.value))),
         count = count + 1,
         aggregatedLastUpdated = aggLastUpdated,
         emvOpt = Some(updatedEmv))
  }

}
