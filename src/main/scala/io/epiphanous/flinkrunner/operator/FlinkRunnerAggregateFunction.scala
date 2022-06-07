package io.epiphanous.flinkrunner.operator

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.aggregate.{
  Aggregate,
  AggregateAccumulator,
  AggregateType
}
import io.epiphanous.flinkrunner.model.{FlinkEvent, UnitMapper}
import org.apache.flink.api.common.functions.AggregateFunction
import squants.{Dimension, Quantity, UnitOfMeasure}

import java.time.Instant
import scala.util.Try

/** A general AggregationFunction for building aggregating state around
  * FlinkRunner Aggregates.
  * @param aggType
  *   the name of Aggregate
  * @param dimension
  *   the dimension of the aggregate's value
  * @param unit
  *   the unit of the aggregate's value
  * @param params
  *   parameters to pass to the aggregate's constructor
  * @param unitMapper
  *   a unit mapper to turn strings into UnitOfMeasures
  * @tparam A
  *   the quantity type
  * @tparam AGG
  *   the Aggregate class type
  */
class FlinkRunnerAggregateFunction[
    E <: ADT,
    A <: Quantity[A],
    AGG <: Aggregate,
    ADT <: FlinkEvent](
    getValueAndTime: E => Try[(A, Instant)],
    aggType: AggregateType,
    dimension: Dimension[A],
    unit: UnitOfMeasure[A],
    params: Map[String, String] = Map.empty,
    unitMapper: UnitMapper = UnitMapper.defaultUnitMapper)
    extends AggregateFunction[E, AggregateAccumulator[AGG], AGG]
    with LazyLogging {

  override def createAccumulator(): AggregateAccumulator[AGG] =
    AggregateAccumulator(
      Aggregate(
        aggType.entryName,
        dimension.name,
        unit.symbol,
        params = params
      )
        .asInstanceOf[AGG]
    )

  override def add(
      element: E,
      holder: AggregateAccumulator[AGG]): AggregateAccumulator[AGG] = {
    (for {
      (quantity, timestamp) <- getValueAndTime(element)
      newAggregate <-
        holder.aggregate
          .update(quantity, timestamp, unitMapper)
          .map(_.asInstanceOf[AGG])
    } yield newAggregate)
      .map(holder.setAggregate)
      .get // this throws any error from the update
  }

  override def getResult(accumulator: AggregateAccumulator[AGG]): AGG =
    accumulator.aggregate

  override def merge(
      a: AggregateAccumulator[AGG],
      b: AggregateAccumulator[AGG]): AggregateAccumulator[AGG] =
    a.setAggregate(a.aggregate.merge(b.aggregate))

}
