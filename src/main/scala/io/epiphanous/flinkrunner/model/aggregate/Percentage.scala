package io.epiphanous.flinkrunner.model.aggregate

import io.epiphanous.flinkrunner.model.UnitMapper
import squants.{Percent, Quantity}

import java.time.Instant
import scala.util.Try

final case class Percentage(
    dimension: String,
    unit: String,
    value: Double = 0d,
    count: BigInt = BigInt(0),
    aggregatedLastUpdated: Instant = Instant.EPOCH,
    lastUpdated: Instant = Instant.now(),
    dependentAggregations: Map[String, Aggregate] =
      Map.empty[String, Aggregate],
    params: Map[String, String] = Map("base" -> Percentage.defaultBase))
    extends Aggregate {

  override def isDimensionless = true

  override def outUnit: String = Percent.symbol

  val baseParam: Double =
    params.getOrElse("base", Percentage.defaultBase).toDouble

  def baseQuantity[A <: Quantity[A]](
      q: A,
      unitMapper: UnitMapper): Try[A] =
    unitMapper.createQuantity(q.dimension, baseParam, unit)

  override def update[A <: Quantity[A]](
      q: A,
      aggLU: Instant,
      unitMapper: UnitMapper): Try[Percentage] = {
    baseQuantity(q, unitMapper).map(b => q / b) map { d =>
      copy(
        value = this.value + d * 100,
        count = count + 1,
        aggregatedLastUpdated = aggLU
      )
    }
  }

}

object Percentage {
  final val DEFAULT_BASE = 1d

  def defaultBase: String = DEFAULT_BASE.toString

  def apply(dimension: String, unit: String, base: Double): Percentage =
    Percentage(dimension, unit, params = Map("base" -> base.toString))
}
