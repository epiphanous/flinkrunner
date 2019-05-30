package io.epiphanous.flinkrunner.model.aggregate

import java.time.Instant

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.UnitMapper
import squants.Quantity

trait Aggregate extends Product with Serializable with LazyLogging {
  def name: String
  def dimension: String
  def unit: String
  def value: Double
  def count: BigInt
  def aggregatedLastUpdated: Instant
  def lastUpdated: Instant
  def dependentAggregations: Map[String, Aggregate]
  def params: Map[String, Any]

  // a copy constructor
  private def _copy(
    value: Double,
    aggregatedLastUpdated: Instant,
    dependentAggregations: Map[String, Aggregate]
  ): Aggregate =
    Aggregate(this.name,
              this.dimension,
              this.unit,
              value,
              this.count + 1,
              aggregatedLastUpdated,
              Instant.now(),
              dependentAggregations,
              this.params)

  /**
    * Used by some subclasses to update the underlying aggregate value as a Quantity.
    * When this is called, any dependent aggregations will be updated and passed into
    * the depAggs parameter. You can find the previous dependent aggregations in
    * `this.dependentAggregations` if you need them.
    *
    * @param current Quantity value of the aggregate
    * @param quantity Quantity the new quantity to incorporate into the aggregate
    * @param depAggs  dependent aggregations already updated with the new quantity
    * @tparam A the dimension of the quantity
    * @return A
    */
  def updateQuantity[A <: Quantity[A]](current: A, quantity: A, depAggs: Map[String, Aggregate]): A = ???

  /**
    * Update dependent aggregations.
    * @param q the quantity being added to the aggregations
    * @param aggLU the instant associated with the new quantity
    * @param unitMapper a unit mapper
    * @tparam A the type of the quantity
    * @return
    */
  def updateDependents[A <: Quantity[A]](q: A, aggLU: Instant, unitMapper: UnitMapper): Map[String, Aggregate] =
    getDependents
      .map(kv => kv._1 -> kv._2.update(q, aggLU, unitMapper))
      .filter(_._2.nonEmpty)
      .map(kv => kv._1 -> kv._2.get)

  def getDependents: Map[String, Aggregate] = this.dependentAggregations

  /**
    * Update the aggregate with a Quantity.
    * @param q Quantity[A]
    * @param aggLU event timestamp of quantity
    * @tparam A dimension of Quantity
    * @return Aggregate
    */
  def update[A <: Quantity[A]](q: A, aggLU: Instant, unitMapper: UnitMapper): Option[Aggregate] = {
    if (q.dimension.name != dimension) {
      logger.error(s"$name[$dimension,$unit] can not be updated with (Quantity[${q.dimension.name}]=$q)")
      None
    } else {
      val depAggs = updateDependents(q, aggLU, unitMapper)
      if (depAggs.size < this.dependentAggregations.size) {
        logger.error(s"$name[$dimension,$unit] dependents can not be updated with (Quantity[${q.dimension.name}]=$q)")
        None
      } else {
        unitMapper
          .createQuantity(q.dimension, value, unit)
          .map(current => updateQuantity(current, q, depAggs) in current.unit) match {
          case Some(updated) =>
            Some(_copy(updated.value, aggLU, depAggs))
          case None =>
            logger.error(s"$name[$dimension,$unit] can not be updated with (Quantity[${q.dimension.name}]=$q)")
            None
        }
      }
    }
  }

  /**
    * Most common entry point for updating aggregates.
    *
    * @param value Double value of quantity to update aggregate with
    * @param unit String unit of quantity to update aggregate with
    * @param aggLU event timestamp of value
    * @param unitMapper allows caller to customize unit system mappings
    * @return
    */
  def update(
    value: Double,
    unit: String,
    aggLU: Instant,
    unitMapper: UnitMapper = UnitMapper.defaultUnitMapper
  ): Option[Aggregate] =
    unitMapper.updateAggregateWith(this, value, unit, aggLU)

  def isEmpty: Boolean = count == BigInt(0)
  def isDefined: Boolean = !isEmpty
  def nonEmpty: Boolean = !isEmpty
  override def toString = f"$value%f $unit"
  def labeledValue = s"$name: $toString"
}

object Aggregate {

  def apply(
    name: String,
    dimension: String,
    unit: String,
    value: Double = 0,
    count: BigInt = BigInt(0),
    aggregatedLastUpdated: Instant = Instant.EPOCH,
    lastUpdated: Instant = Instant.now(),
    dependentAggregations: Map[String, Aggregate] = Map.empty[String, Aggregate],
    params: Map[String, Any] = Map.empty[String, Any],
    alpha: Option[Double] = None,
    base: Option[Double] = None
  ): Aggregate = {
    val initValue = if (name == "Min" && count == 0 && value == 0) Double.MaxValue else value
    name match {
      case "Mean" =>
        Mean(dimension, unit, value, name, count, aggregatedLastUpdated, lastUpdated)
      case "Count" =>
        Count("Dimensionless", "ea", value, name, count, aggregatedLastUpdated, lastUpdated)
      case "ExponentialMovingAverage" =>
        ExponentialMovingAverage(dimension,
                                 unit,
                                 value,
                                 name,
                                 count,
                                 aggregatedLastUpdated,
                                 lastUpdated,
                                 Map.empty[String, Aggregate],
                                 maybeUpdateParams(params, "alpha", alpha, 0.7))

      case "ExponentialMovingStandardDeviation" =>
        ExponentialMovingStandardDeviation(dimension,
                                           unit,
                                           value,
                                           name,
                                           count,
                                           aggregatedLastUpdated,
                                           lastUpdated,
                                           dependentAggregations,
                                           maybeUpdateParams(params, "alpha", alpha, 0.7))

      case "ExponentialMovingVariance" =>
        ExponentialMovingVariance(dimension,
                                  unit,
                                  value,
                                  name,
                                  count,
                                  aggregatedLastUpdated,
                                  lastUpdated,
                                  dependentAggregations,
                                  maybeUpdateParams(params, "alpha", alpha, 0.7))

      case "Histogram" =>
        Histogram(dimension, unit, value, name, count, aggregatedLastUpdated, lastUpdated, dependentAggregations)

      case "Max" =>
        Max(dimension, unit, value, name, count, aggregatedLastUpdated, lastUpdated)

      case "Min" =>
        Min(dimension, unit, initValue, name, count, aggregatedLastUpdated, lastUpdated)

      case "Range" =>
        Range(dimension, unit, value, name, count, aggregatedLastUpdated, lastUpdated, dependentAggregations)

      case "Sum" =>
        Sum(dimension, unit, value, name, count, aggregatedLastUpdated, lastUpdated)

      case "Variance" =>
        Variance(dimension, unit, value, name, count, aggregatedLastUpdated, lastUpdated, dependentAggregations)

      case "StandardDeviation" =>
        StandardDeviation(dimension,
                          unit,
                          value,
                          name,
                          count,
                          aggregatedLastUpdated,
                          lastUpdated,
                          dependentAggregations)

      case "SumOfSquaredDeviations" =>
        SumOfSquaredDeviations(dimension,
                               unit,
                               value,
                               name,
                               count,
                               aggregatedLastUpdated,
                               lastUpdated,
                               dependentAggregations)
      case "Percentage" =>
        Percentage(dimension,
                   unit,
                   value,
                   name,
                   count,
                   aggregatedLastUpdated,
                   lastUpdated,
                   dependentAggregations,
                   maybeUpdateParams(params, "base", base, 1d))

      case _ => throw new UnsupportedOperationException(s"Unknown aggregation type '$name'")
    }
  }

  def maybeUpdateParams[T](map: Map[String, Any], key: String, value: Option[T], defaultValue: T): Map[String, Any] =
    if (map.contains(key)) map else map.updated(key, value.getOrElse(map.getOrElse(key, defaultValue)))
}
