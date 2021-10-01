package io.epiphanous.flinkrunner.util

import io.epiphanous.flinkrunner.model.FlinkEvent
import org.apache.flink.api.common.eventtime.{
  WatermarkGeneratorSupplier,
  WatermarkStrategy
}

import java.time.Duration

class BoundedLatenessWatermarkStrategy[E <: FlinkEvent](
    val maxAllowedLateness: Duration,
    val streamID: String)
    extends WatermarkStrategy[E] {
  override def createWatermarkGenerator(
      context: WatermarkGeneratorSupplier.Context) =
    new BoundedLatenessGenerator(maxAllowedLateness.toMillis, streamID)
}
