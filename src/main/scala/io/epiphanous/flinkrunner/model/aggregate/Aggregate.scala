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
    * Update the aggregate with a Quantity.
    * @param q Quantity[A]
    * @param aggLU event timestamp of quantity
    * @tparam A dimension of Quantity
    * @return Aggregate
    */
  def update[A <: Quantity[A]](q: A, aggLU: Instant): Option[Aggregate] = {
    if (q.dimension.name != dimension) {
      logger.error(s"$name[$dimension,$unit] can not be updated with (Quantity[${q.dimension.name}]=$q)")
      None
    } else {
      val depAggs = this.dependentAggregations
        .map(kv => kv._1 -> kv._2.update(q, aggLU))
        .filter(kv => kv._2.nonEmpty)
        .map(kv => kv._1 -> kv._2.get)
      if (depAggs.size < this.dependentAggregations.size) {
        logger.error(s"$name[$dimension,$unit] dependents can not be updated with (Quantity[${q.dimension.name}]=$q)")
        None
      } else {
        q.dimension
          .symbolToUnit(unit)
          .map(u => u(value))
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
    alpha: Double = 0.7
  ): Aggregate = {
    name match {
      case "Mean" =>
        Mean(dimension, unit, value, name, count, aggregatedLastUpdated, lastUpdated)
      case "Count" =>
        Count(dimension, unit, value, name, count, aggregatedLastUpdated, lastUpdated)
      case "ExponentialMovingAverage" =>
        ExponentialMovingAverage(dimension,
                                 unit,
                                 value,
                                 name,
                                 count,
                                 aggregatedLastUpdated,
                                 lastUpdated,
                                 Map.empty[String, Aggregate],
                                 params)

      case "ExponentialMovingStandardDeviation" =>
        ExponentialMovingStandardDeviation(dimension,
                                           unit,
                                           value,
                                           name,
                                           count,
                                           aggregatedLastUpdated,
                                           lastUpdated,
                                           dependentAggregations,
                                           params)

      case "ExponentialMovingVariance" =>
        ExponentialMovingVariance(dimension,
                                  unit,
                                  value,
                                  name,
                                  count,
                                  aggregatedLastUpdated,
                                  lastUpdated,
                                  dependentAggregations,
                                  params)

      case "Histogram" =>
        Histogram(dimension, unit, value, name, count, aggregatedLastUpdated, lastUpdated, dependentAggregations)

      case "Max" =>
        Max(dimension, unit, value, name, count, aggregatedLastUpdated, lastUpdated)

      case "Min" =>
        Min(dimension, unit, value, name, count, aggregatedLastUpdated, lastUpdated)

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

      case _ => throw new UnsupportedOperationException(s"Unknown aggregation type '$name'")
    }
  }
}
