package io.epiphanous.flinkrunner.model.aggregate

import java.time.Instant

import io.epiphanous.flinkrunner.model.UnitMapper
import squants.Quantity

final case class Percentage(
  dimension: String,
  unit: String,
  value: Double = 0d,
  name: String = "Percentage",
  count: BigInt = BigInt(0),
  aggregatedLastUpdated: Instant = Instant.EPOCH,
  lastUpdated: Instant = Instant.now(),
  dependentAggregations: Map[String, Aggregate] = Map.empty[String, Aggregate],
  params: Map[String, Any] = Map("base" -> 1d))
    extends Aggregate {

  val baseParam: Double = params.getOrElse("base", 1d).asInstanceOf[Double]

  def baseQuantity[A <: Quantity[A]](q: A, unitMapper: UnitMapper) =
    unitMapper.createQuantity(q.dimension, baseParam, unit)

  override def update[A <: Quantity[A]](q: A, aggLU: Instant, unitMapper: UnitMapper) = {
    val updateValue = baseQuantity(q, unitMapper).map(b => q / b) match {
      case Some(addValue) => addValue
      case None =>
        logger.error(s"$name[$dimension,$unit] can not be updated with (Quantity[${q.dimension.name}]=$q)")
        0d
    }
    Some(copy(value = this.value + updateValue, count = count + 1, aggregatedLastUpdated = aggLU))
  }

}

object Percentage {
  def apply(dimension: String, unit: String, base: Double): Percentage =
    Percentage(dimension, unit, params = Map("base" -> base))
}
