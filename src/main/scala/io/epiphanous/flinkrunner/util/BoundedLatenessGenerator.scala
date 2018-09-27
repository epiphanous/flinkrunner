package io.epiphanous.flinkrunner.util

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkEvent
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class BoundedLatenessGenerator[E <: FlinkEvent]( /*val getTimestamp: E => Long,*/ val maxAllowedLateness: Long)
    extends AssignerWithPeriodicWatermarks[E]
    with LazyLogging {

  var latestTotal         = 0L
  var latestAllowed       = 0L
  var mostRecentTimestamp = 0L

  override def extractTimestamp(event: E, previousElementTimestamp: Long): Long = {
    val timestamp = event.$timestamp
    if (timestamp < mostRecentTimestamp) {
      val lateness = mostRecentTimestamp - timestamp
      if (lateness <= maxAllowedLateness)
        latestAllowed = Math.max(lateness, latestAllowed)
      latestTotal = Math.max(lateness, latestTotal)
    }
    mostRecentTimestamp = Math.max(timestamp, mostRecentTimestamp)
    timestamp
  }

  override def getCurrentWatermark: Watermark =
    new Watermark(mostRecentTimestamp - maxAllowedLateness)

}
