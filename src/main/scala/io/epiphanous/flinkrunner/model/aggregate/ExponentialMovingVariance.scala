package io.epiphanous.flinkrunner.model.aggregate
import java.time.Instant

import squants.{Quantity, UnitOfMeasure}

final case class ExponentialMovingVariance[A <: Quantity[A]](
  unit: UnitOfMeasure[A],
  value: Option[A] = None,
  count: BigInt = BigInt(0),
  name: String = "EMV",
  aggregatedLastUpdated: Instant = Instant.now(),
  lastUpdated: Instant = Instant.now(),
  emaOpt: Option[ExponentialMovingAverage[A]] = None,
  alpha: Double = 0.7)
    extends Aggregate[A] {

  def ema = emaOpt.getOrElse(ExponentialMovingAverage(unit))

  override def update(q: A, aggLastUpdated: Instant) = {
    val currentEma = ema
    val updatedEma = currentEma.update(q, aggLastUpdated)
    val delta = q - currentEma.getValue
    copy(
      value = Some(
        value
          .map(ma => (1 - alpha) * (ma + alpha * delta.value * delta))
          .getOrElse(q)
      ),
      count = count + 1,
      aggregatedLastUpdated = aggLastUpdated,
      emaOpt = Some(updatedEma)
    )
  }

}
