package io.epiphanous.flinkrunner.model.aggregate

import squants.Quantity

import java.time.Instant

final case class ExponentialMovingStandardDeviation(
    dimension: String,
    unit: String,
    value: Double = 0d,
    count: BigInt = BigInt(0),
    aggregatedLastUpdated: Instant = Instant.EPOCH,
    lastUpdated: Instant = Instant.now(),
    dependentAggregations: Map[String, Aggregate] =
      Map.empty[String, Aggregate],
    params: Map[String, String] = Map(
      "alpha" -> ExponentialMovingStandardDeviation.defaultAlpha
    ))
    extends Aggregate {

  override def getDependents: Map[String, Aggregate] = {
    if (this.dependentAggregations.isEmpty)
      Map(
        "ExponentialMovingVariance" -> ExponentialMovingVariance(
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
    if (count == 0) current.unit(0d)
    else {
      val updatedEmv = depAggs("ExponentialMovingVariance")
      current.unit(Math.sqrt(updatedEmv.value))
    }
  }

}

object ExponentialMovingStandardDeviation {
  private val DEFAULT_ALPHA = 0.7

  def defaultAlpha: String = DEFAULT_ALPHA.toString

  def apply(
      dimension: String,
      unit: String,
      alpha: Double): ExponentialMovingStandardDeviation =
    ExponentialMovingStandardDeviation(
      dimension,
      unit,
      dependentAggregations = Map(
        "ExponentialMovingVariance" -> ExponentialMovingVariance(
          dimension,
          unit,
          alpha
        )
      ),
      params = Map("alpha" -> alpha.toString)
    )
}
