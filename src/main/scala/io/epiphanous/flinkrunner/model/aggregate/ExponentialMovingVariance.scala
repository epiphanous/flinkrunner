package io.epiphanous.flinkrunner.model.aggregate

import squants.Quantity

import java.time.Instant

final case class ExponentialMovingVariance(
    dimension: String,
    unit: String,
    value: Double = 0d,
    count: BigInt = BigInt(0),
    aggregatedLastUpdated: Instant = Instant.EPOCH,
    lastUpdated: Instant = Instant.now(),
    dependentAggregations: Map[String, Aggregate] =
      Map.empty[String, Aggregate],
    params: Map[String, String] = Map(
      "alpha" -> ExponentialMovingVariance.defaultAlpha
    ))
    extends Aggregate {

  def alpha: Double = params
    .getOrElse("alpha", ExponentialMovingVariance.defaultAlpha)
    .toDouble

  override def getDependents: Map[String, Aggregate] = {
    if (this.dependentAggregations.isEmpty)
      Map(
        "ExponentialMovingAverage" -> ExponentialMovingAverage(
          dimension,
          unit,
          params = params
        )
      )
    else this.dependentAggregations
  }

  override def updateQuantity[A <: Quantity[A]](
      current: A,
      quantity: A,
      depAggs: Map[String, Aggregate]): A = {
    if (count == 0) quantity.unit(0d)
    else {
      val currentEma = getDependents("ExponentialMovingAverage")
      val q          = quantity in current.unit
      val delta      = q - current.unit(currentEma.value)
      (1 - alpha) * (current + delta * delta.value * alpha)
    }
  }

}

object ExponentialMovingVariance {
  final val DEFAULT_ALPHA = 0.7

  def defaultAlpha: String = DEFAULT_ALPHA.toString

  def apply(
      dimension: String,
      unit: String,
      alpha: Double): ExponentialMovingVariance =
    ExponentialMovingVariance(
      dimension,
      unit,
      params = Map("alpha" -> alpha.toString)
    )

}
