package io.epiphanous.flinkrunner.util

import io.epiphanous.flinkrunner.model.FlinkEvent

case class TestEvent(id: String, timestamp: Long) extends FlinkEvent {
  override def $id = id

  override def $key = id

  override def $timestamp = timestamp
}
