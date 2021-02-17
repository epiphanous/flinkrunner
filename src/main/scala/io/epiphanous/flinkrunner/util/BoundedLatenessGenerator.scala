package io.epiphanous.flinkrunner.util

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkEvent
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

import java.time.Instant

class BoundedLatenessGenerator[E <: FlinkEvent](val maxAllowedLateness: Long, val streamID: String)
  extends AssignerWithPeriodicWatermarks[E]
    with LazyLogging {

  var firstTimestamp = 0L
  var latestTotal = 0L
  var latestAllowed = 0L
  var mostRecentTimestamp = 0L
  var largestGap = 0L

  override def extractTimestamp(event: E, previousElementTimestamp: Long): Long = {
    val timestamp = event.$timestamp
    if (firstTimestamp == 0) {
      firstTimestamp = timestamp;
      logger.debug(s"$streamID first event seen has $firstTimestamp millis");
    }
    val gap = if (previousElementTimestamp > 0) timestamp - previousElementTimestamp else 0;
    val now = System.currentTimeMillis();
    if (gap > largestGap) {
      largestGap = gap;

      logger.debug(s"$streamID largest gap since last event is $largestGap millis")
    }
    // check lateness
    if (timestamp < mostRecentTimestamp) {
      val lateness = mostRecentTimestamp - timestamp
      val allowed = lateness <= maxAllowedLateness
      val lateTags = new StringBuilder(if (allowed) "ALLOWED" else "TOO LATE")
      if (allowed && lateness > latestAllowed) {
        lateTags ++= ", LATEST ALLOWED"
      }
      if (lateness > latestTotal) {
        lateTags ++= ", LATEST SO FAR"
      }
      if (allowed) {
        latestAllowed = Math.max(lateness, latestAllowed)
      }
      latestTotal = Math.max(lateness, latestTotal)
      logger.info(s"$streamID event is $lateness millis late [${lateTags.mkString}]")
    }
    // warn about future timestamps
    else if (timestamp > now) {
      logger.warn(s"$streamID event has timestamp ${timestamp - now} millis in future")
    }
    // update most recent timestamp
    else {
      mostRecentTimestamp = Math.max(timestamp, mostRecentTimestamp)
    }
    timestamp
  }

  override def getCurrentWatermark: Watermark = {
    val watermarkTime = Math.max(0L, mostRecentTimestamp - maxAllowedLateness)
    logger.debug(s"$streamID WATERMARK ${watermarkTime} ${Instant.ofEpochMilli(watermarkTime)}")
    new Watermark(watermarkTime);
  }

}
