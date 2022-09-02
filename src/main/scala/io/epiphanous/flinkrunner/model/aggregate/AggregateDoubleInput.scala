package io.epiphanous.flinkrunner.model.aggregate

import java.time.Instant

/** A possible input to a FlinkRunnerAggregateFunction's add method.
  * @param value
  *   the value to add as a double
  * @param unit
  *   the unit of the value, as a string
  * @param aggLU
  *   the instant of time this update occurred
  */
case class AggregateDoubleInput(
    value: Double,
    unit: String,
    aggLU: Instant)
