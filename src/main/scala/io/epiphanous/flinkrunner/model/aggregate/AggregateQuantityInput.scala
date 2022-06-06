package io.epiphanous.flinkrunner.model.aggregate

import squants.Quantity

import java.time.Instant

/** A possible input to a FlinkRunnerAggregateFunction's add method.
  * @param quantity
  *   the value/unit to add to the aggregate as an A
  * @param aggLU
  *   the time associated with the update
  * @tparam A
  *   the quantity type
  */
case class AggregateQuantityInput[A <: Quantity[A]](
    quantity: A,
    aggLU: Instant)
