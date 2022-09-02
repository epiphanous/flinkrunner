package io.epiphanous.flinkrunner.model.aggregate

case class AggregateAccumulator[AGG <: Aggregate](var aggregate: AGG) {
  def setAggregate(updatedAggregate: AGG): AggregateAccumulator[AGG] = {
    aggregate = updatedAggregate
    this
  }
}
