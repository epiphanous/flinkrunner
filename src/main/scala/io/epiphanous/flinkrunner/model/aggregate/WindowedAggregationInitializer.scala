package io.epiphanous.flinkrunner.model.aggregate

import io.epiphanous.flinkrunner.model.{FlinkEvent, UnitMapper}
import io.epiphanous.flinkrunner.operator.FlinkRunnerAggregateFunction
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.windows.Window
import squants.{Dimension, Quantity, UnitOfMeasure}

import java.time.{Duration, Instant}
import scala.util.Try

/** A value class to holder initialization information for a windowed
  * stream aggregation.
  *
  * @param windowAssigner
  *   the WindowAssigner[E,W] for windowing the source stream
  * @param allowedLateness
  *   a duration describing how late events may arrive and be included in
  *   the current window
  * @param extractValueAndTime
  *   a function to extract the value to be aggregate, as well as its event
  *   time, from the underlying source event
  * @param aggType
  *   the type of aggregation (as an AggregateType enum)
  * @param dimension
  *   the dimension of the aggregation
  * @param unit
  *   the preferred unit of measure for the aggregation
  * @param params
  *   any parameters to initialize the Aggregate
  * @param unitMapper
  *   a unit mapper to help construct quantities
  * @tparam E
  *   the source event type
  * @tparam WINDOW
  *   the type of window
  * @tparam AGG
  *   the type of Aggregate
  * @tparam A
  *   the type of quantity being aggregated
  * @tparam ADT
  *   the flinkrunner algebraic data type
  */
case class WindowedAggregationInitializer[
    E <: ADT,
    KEY, // input key key type
    WINDOW <: Window,
    AGG <: Aggregate,
    A <: Quantity[A],
    OUT, // output of process window function
    ADT <: FlinkEvent](
    windowAssigner: WindowAssigner[E, WINDOW],
    allowedLateness: Duration,
    extractValueAndTime: E => Try[(A, Instant)],
    aggType: AggregateType,
    dimension: Dimension[A],
    unit: UnitOfMeasure[A],
    params: Map[String, String] = Map.empty,
    unitMapper: UnitMapper = UnitMapper.defaultUnitMapper,
    processWindowFunction: ProcessWindowFunction[AGG, OUT, KEY, WINDOW]
) {
  val aggregateFunction = new FlinkRunnerAggregateFunction[E, A, AGG, ADT](
    extractValueAndTime,
    aggType,
    dimension,
    unit,
    params,
    unitMapper
  )
}
