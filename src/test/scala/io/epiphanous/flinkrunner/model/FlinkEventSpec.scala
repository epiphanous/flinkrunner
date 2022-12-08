package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.PropSpec
class FlinkEventSpec extends PropSpec {

  case class TestStringInterpolationEvent(id: String, timestamp: Long)
      extends FlinkEvent {
    override def $id        = id
    override def $key       = id
    override def $timestamp = timestamp
  }

  property("string interpolation") {
    val event    = new TestStringInterpolationEvent("test_id", 0)
    val bucketId = event.$bucketId
    bucketId should include("test_id")
    bucketId should not contain "$key"
  }
}
