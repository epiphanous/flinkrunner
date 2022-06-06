package io.epiphanous.flinkrunner.operator

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.UnitMapper
import io.epiphanous.flinkrunner.model.aggregate.{
  Aggregate,
  AggregateDoubleInput,
  AggregateQuantityInput,
  AggregateType
}
import org.apache.flink.api.common.functions.RichAggregateFunction
import squants.{Dimension, Quantity, UnitOfMeasure}

/** A general RichAggregationFunction for building aggregating state around
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
  * @tparam ACC
  *   the Aggregate class type
  */
class FlinkRunnerAggregateFunction[A <: Quantity[A], ACC <: Aggregate](
    aggType: AggregateType,
    dimension: Dimension[A],
    unit: UnitOfMeasure[A],
    params: Map[String, Any] = Map.empty,
    unitMapper: UnitMapper = UnitMapper.defaultUnitMapper)
    extends RichAggregateFunction[Either[AggregateQuantityInput[
      A
    ], AggregateDoubleInput], ACC, ACC]
    with LazyLogging {

  override def createAccumulator(): ACC =
    Aggregate(aggType.entryName, dimension.name, unit.symbol)
      .asInstanceOf[ACC]

  override def add(
      value: Either[AggregateQuantityInput[A], AggregateDoubleInput],
      accumulator: ACC): ACC = (value match {
    case Left(qi)  => accumulator.update(qi.quantity, qi.aggLU, unitMapper)
    case Right(di) => accumulator.update(di.value, di.unit, di.aggLU)
  }).getOrElse(accumulator).asInstanceOf[ACC]

  override def getResult(accumulator: ACC): ACC = accumulator

  // TODO: need to figure this out
  override def merge(a: ACC, b: ACC): ACC = ???

}
