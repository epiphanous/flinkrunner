package io.epiphanous.flinkrunner.model

import java.time.Instant

sealed trait MySimpleADT extends FlinkEvent

case class SimpleA(id: String, a0: String, a1: Int, ts: Instant)
    extends MySimpleADT {
  override def $id: String = id

  override def $key: String = a0

  override def $timestamp: Long = ts.toEpochMilli
}

case class SimpleB(
    id: String,
    b0: String,
    b1: Double,
    b2: Option[Int],
    ts: Instant)
    extends MySimpleADT {
  override def $id: String = id

  override def $key: String = b0

  override def $timestamp: Long = ts.toEpochMilli
}

case class SimpleC(
    id: String,
    c1: String,
    c2: Double,
    c3: Int,
    ts: Instant)
    extends MySimpleADT {
  override def $id: String = id

  override def $key: String = c1

  override def $timestamp: Long = ts.toEpochMilli
}
