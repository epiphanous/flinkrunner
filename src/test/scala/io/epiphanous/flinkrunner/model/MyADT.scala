package io.epiphanous.flinkrunner.model

import java.time.Instant
import java.util.UUID

sealed trait MyADT extends FlinkEvent

case class A(
    id: String = UUID.randomUUID().toString,
    a: String = "A",
    value: Int = 0,
    modified: Instant)
    extends MyADT {
  override def $id: String      = id
  override def $key: String     = a
  override def $timestamp: Long = modified.toEpochMilli
}

case class B(
    id: String = UUID.randomUUID().toString,
    b: String = "B",
    value: Double = 0d,
    modified: Instant)
    extends MyADT {
  override def $id: String      = id
  override def $key: String     = b
  override def $timestamp: Long = modified.toEpochMilli
}
