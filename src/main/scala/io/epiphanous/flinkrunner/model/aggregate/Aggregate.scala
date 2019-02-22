package io.epiphanous.flinkrunner.model.aggregate

import java.time.Instant

import squants.{Quantity, UnitOfMeasure}

trait Aggregate[A <: Quantity[A]] {
  def unit: UnitOfMeasure[A]
  def value: Option[A]
  def count: BigInt
  def name: String
  def aggregatedLastUpdated: Instant
  def lastUpdated: Instant

  def update(q: A, aggregatedLastUpdated: Instant): Aggregate[A]
  def isEmpty: Boolean = value.isEmpty
  def isDefined: Boolean = value.isDefined
  def nonEmpty: Boolean = value.nonEmpty
  def getValue = value.getOrElse(unit(0)) in unit
  override def toString = getValue.toString
  def toString(otherUnit: UnitOfMeasure[A]) = getValue.toString(otherUnit)
  def labeledValue = s"$name: $toString"
  def labeledValue(otherUnit: UnitOfMeasure[A]) =
    s"$name: ${toString(otherUnit)}"
}
