package io.epiphanous.flinkrunner.model.aggregate

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.UnitMapper
import squants.Quantity

import java.time.Instant
import scala.util.{Failure, Success, Try}

trait Aggregate extends Product with Serializable with LazyLogging {

  def name: String = getClass.getSimpleName

  def dimension: String

  def unit: String

  def value: Double

  def count: BigInt

  def aggregatedLastUpdated: Instant

  def lastUpdated: Instant

  def dependentAggregations: Map[String, Aggregate]

  def params: Map[String, String]

  def isDimensionless: Boolean = false

  def outUnit: String = unit

  /** Merge another aggregate of the same type into this one.
    * @param other
    *   the other aggregate
    * @tparam AGG
    *   the type of aggregate
    * @return
    *   a merged aggregate
    */
  def merge[AGG <: Aggregate](other: AGG): AGG = ???

  // a copy constructor
  private def _copy(
      newValue: Double,
      aggregatedLastUpdated: Instant,
      dependentAggregations: Map[String, Aggregate]
  ): Aggregate =
    Aggregate(
      name,
      dimension,
      outUnit,
      newValue,
      count + 1,
      aggregatedLastUpdated,
      Instant.now(),
      dependentAggregations,
      params
    )

  /** Used by some subclasses to update the underlying aggregate value as a
    * Quantity. When this is called, any dependent aggregations will be
    * updated and passed into the depAggs parameter. You can find the
    * previous dependent aggregations in `this.dependentAggregations` if
    * you need them.
    *
    * @param current
    *   Quantity value of the aggregate
    * @param quantity
    *   Quantity the new quantity to incorporate into the aggregate
    * @param depAggs
    *   dependent aggregations already updated with the new quantity
    * @tparam A
    *   the dimension of the quantity
    * @return
    *   A
    */
  def updateQuantity[A <: Quantity[A]](
      current: A,
      quantity: A,
      depAggs: Map[String, Aggregate]): A = ???

  /** Update dependent aggregations.
    *
    * @param q
    *   the quantity being added to the aggregations
    * @param aggLU
    *   the instant associated with the new quantity
    * @param unitMapper
    *   a unit mapper
    * @tparam A
    *   the type of the quantity
    * @return
    */
  def updateDependents[A <: Quantity[A]](
      q: A,
      aggLU: Instant,
      unitMapper: UnitMapper): Try[Map[String, Aggregate]] = Try {
    val split =
      getDependents
        .mapValues(agg => agg.update(q, aggLU, unitMapper))
        .groupBy { case (_, v) =>
          if (v.isFailure) "f" else "s"
        }
    if (split.isEmpty) Map.empty[String, Aggregate]
    else if (split.contains("f")) {
      val fails = split("f")
      throw new RuntimeException(
        s"""
           |update failed for ${if (
            fails.size < dependentAggregations.size
          ) "some"
          else "all"} dependents: (${fails.keySet})
           |${fails
            .map {
              case (k, Failure(exception)) =>
                s"$k: ${exception.getMessage}"
              case _                       => "wont happen"
            }
            .mkString("=====\n  - ", "\n  - ", "\n=====")}
           |""".stripMargin
      )
    } else split("s").collect { case (k, Success(agg)) => (k, agg) }
  }

  def getDependents: Map[String, Aggregate] = dependentAggregations

  /** Update the aggregate with a Quantity.
    *
    * @param q
    *   Quantity[A]
    * @param aggLU
    *   event timestamp of quantity
    * @tparam A
    *   dimension of Quantity
    * @return
    *   Aggregate
    */
  def update[A <: Quantity[A]](
      q: A,
      aggLU: Instant,
      unitMapper: UnitMapper): Try[Aggregate] = {
    if (q.dimension.name != dimension) {
      Failure(
        new RuntimeException(
          s"$name[$dimension,$unit] can not be updated with (Quantity[${q.dimension.name}]=$q)"
        )
      )

    } else {
      for {
        deps <- updateDependents(q, aggLU, unitMapper)
        current <- unitMapper.createQuantity(q.dimension, value, unit)
        updated <-
          Success(
            (updateQuantity(current, q, deps) in current.unit).value
          )
        agg <- Success(_copy(updated, aggLU, deps))
      } yield agg
    }
  }

  /** Most common entry point for updating aggregates.
    *
    * @param value
    *   Double value of quantity to update aggregate with
    * @param unit
    *   String unit of quantity to update aggregate with
    * @param aggLU
    *   event timestamp of value
    * @param unitMapper
    *   allows caller to customize unit system mappings
    * @return
    */
  def update(
      value: Double,
      unit: String,
      aggLU: Instant,
      unitMapper: UnitMapper = UnitMapper.defaultUnitMapper
  ): Try[Aggregate] =
    unitMapper.updateAggregateWith(this, value, unit, aggLU)

  def isEmpty: Boolean = count == BigInt(0)

  def isDefined: Boolean = !isEmpty

  def nonEmpty: Boolean = !isEmpty

  override def toString = f"$value%f $outUnit"

  def labeledValue = s"$name: $toString"
}

object Aggregate extends LazyLogging {

  implicit class caseOps(s: String) {
    def normalize: String =
      "[^A-Za-z\\d]".r.replaceAllIn(s, "").toLowerCase()
  }

  def apply(
      name: String,
      dimension: String,
      unit: String,
      value: Double = 0,
      count: BigInt = BigInt(0),
      aggregatedLastUpdated: Instant = Instant.EPOCH,
      lastUpdated: Instant = Instant.now(),
      dependentAggregations: Map[String, Aggregate] =
        Map.empty[String, Aggregate],
      params: Map[String, String] = Map.empty[String, String]
  ): Aggregate = {
    val normalizedName = name.normalize
    val initValue      =
      if (normalizedName == "min" && count == 0 && value == 0)
        Double.MaxValue
      else value
    normalizedName match {
      case "mean"                     =>
        Mean(
          dimension,
          unit,
          value,
          count,
          aggregatedLastUpdated,
          lastUpdated
        )
      case "count"                    =>
        Count(
          dimension,
          unit,
          value,
          count,
          aggregatedLastUpdated,
          lastUpdated
        )
      case "exponentialmovingaverage" =>
        ExponentialMovingAverage(
          dimension,
          unit,
          value,
          count,
          aggregatedLastUpdated,
          lastUpdated,
          dependentAggregations,
          maybeUpdateParams(
            params,
            "alpha",
            ExponentialMovingAverage.defaultAlpha
          )
        )

      case "exponentialmovingstandarddeviation" =>
        ExponentialMovingStandardDeviation(
          dimension,
          unit,
          value,
          count,
          aggregatedLastUpdated,
          lastUpdated,
          dependentAggregations,
          maybeUpdateParams(
            params,
            "alpha",
            ExponentialMovingStandardDeviation.defaultAlpha
          )
        )

      case "exponentialmovingvariance" =>
        ExponentialMovingVariance(
          dimension,
          unit,
          value,
          count,
          aggregatedLastUpdated,
          lastUpdated,
          dependentAggregations,
          maybeUpdateParams(
            params,
            "alpha",
            ExponentialMovingVariance.defaultAlpha
          )
        )

      case "histogram" =>
        Histogram(
          dimension,
          unit,
          value,
          count,
          aggregatedLastUpdated,
          lastUpdated,
          dependentAggregations
        )

      case "max" =>
        Max(
          dimension,
          unit,
          value,
          count,
          aggregatedLastUpdated,
          lastUpdated
        )

      case "min" =>
        Min(
          dimension,
          unit,
          initValue,
          count,
          aggregatedLastUpdated,
          lastUpdated
        )

      case "range" =>
        Range(
          dimension,
          unit,
          value,
          count,
          aggregatedLastUpdated,
          lastUpdated,
          dependentAggregations
        )

      case "sum" =>
        Sum(
          dimension,
          unit,
          value,
          count,
          aggregatedLastUpdated,
          lastUpdated
        )

      case "variance" =>
        Variance(
          dimension,
          unit,
          value,
          count,
          aggregatedLastUpdated,
          lastUpdated,
          dependentAggregations
        )

      case "standarddeviation" =>
        StandardDeviation(
          dimension,
          unit,
          value,
          count,
          aggregatedLastUpdated,
          lastUpdated,
          dependentAggregations
        )

      case "sumofsquareddeviations" =>
        SumOfSquaredDeviations(
          dimension,
          unit,
          value,
          count,
          aggregatedLastUpdated,
          lastUpdated,
          dependentAggregations
        )
      case "percentage"             =>
        Percentage(
          dimension,
          unit,
          value,
          count,
          aggregatedLastUpdated,
          lastUpdated,
          dependentAggregations,
          maybeUpdateParams(params, "base", Percentage.defaultBase)
        )

      case _ =>
        val message = s"Unknown aggregation type '$name'"
        logger.error(message)
        throw new UnsupportedOperationException(message)
    }
  }

  def maybeUpdateParams(
      map: Map[String, String],
      key: String,
      defaultValue: String): Map[String, String] =
    if (map.contains(key)) map else map.updated(key, defaultValue)
}
