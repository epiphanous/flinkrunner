package io.epiphanous.flinkrunner.util

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkEvent
import org.apache.flink.api.common.eventtime.{Watermark, WatermarkGenerator, WatermarkOutput}

import java.time.Instant
import scala.collection.mutable

class BoundedLatenessGenerator[E <: FlinkEvent](
    val maxAllowedLateness: Long,
    val streamID: String)
    extends WatermarkGenerator[E]
    with LazyLogging {

  private[this] var _firstTimestamp: Long = 0L

  def firstTimestamp: Long = _firstTimestamp

  private[this] var _latestTotal: Long = 0L

  def latestTotal: Long = _latestTotal

  private[this] var _latestAllowed: Long = 0L

  def latestAllowed: Long = _latestAllowed

  private[this] var _mostRecentTimestamp: Long = 0L

  def mostRecentTimestamp: Long = _mostRecentTimestamp

  private[this] var _largestGap: Long = 0L

  def largestGap: Long = _largestGap

  private[this] var _watermark: Watermark = new Watermark(0)

  def watermark: Watermark = _watermark

  override def onEvent(
      event: E,
      previousElementTimestamp: Long,
      output: WatermarkOutput): Unit = {
    val timestamp = event.$timestamp
    if (firstTimestamp == 0) {
      _firstTimestamp = timestamp
      logger.trace(
        s"$streamID first event seen has $firstTimestamp millis"
      )
    }
    val gap       =
      if (previousElementTimestamp > 0)
        timestamp - previousElementTimestamp
      else 0
    val now       = System.currentTimeMillis()
    if (gap > largestGap) {
      _largestGap = gap

      logger.trace(
        s"$streamID largest gap since last event is $largestGap millis"
      )
    }
    // check lateness
    if (timestamp < mostRecentTimestamp) {
      val lateness = mostRecentTimestamp - timestamp
      val allowed  = lateness <= maxAllowedLateness
      val lateTags = new mutable.StringBuilder(
        if (allowed) "ALLOWED" else "TOO LATE"
      )
      if (allowed && lateness > latestAllowed) {
        lateTags ++= ", LATEST ALLOWED"
      }
      if (lateness > latestTotal) {
        lateTags ++= ", LATEST SO FAR"
      }
      if (allowed) {
        _latestAllowed = Math.max(lateness, latestAllowed)
      }
      _latestTotal = Math.max(lateness, latestTotal)
      logger.info(
        s"$streamID event is $lateness millis late [${lateTags.mkString}]"
      )
    }
    // warn about future timestamps
    else if (timestamp > now) {
      logger.warn(
        s"$streamID event has timestamp ${timestamp - now} millis in future"
      )
    }
    // update most recent timestamp
    else {
      _mostRecentTimestamp = Math.max(timestamp, mostRecentTimestamp)
    }
  }

  override def onPeriodicEmit(output: WatermarkOutput): Unit = {
    val watermarkTime = mostRecentTimestamp - maxAllowedLateness - 1
    if (watermarkTime > 0) {
      logger.trace(
        s"$streamID WATERMARK $watermarkTime ${Instant.ofEpochMilli(watermarkTime)}"
      )
      _watermark = new Watermark(watermarkTime)
      output.emitWatermark(watermark)
    }
  }

}
