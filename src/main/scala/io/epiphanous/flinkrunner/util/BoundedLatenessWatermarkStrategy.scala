package io.epiphanous.flinkrunner.util

import io.epiphanous.flinkrunner.model.FlinkEvent
import org.apache.flink.api.common.eventtime.{
  WatermarkGenerator,
  WatermarkGeneratorSupplier,
  WatermarkStrategy
}

class BoundedLatenessWatermarkStrategy[E <: FlinkEvent](
    val maxAllowedLateness: Long,
    val streamID: String)
    extends WatermarkStrategy[E] {
  override def createWatermarkGenerator(
      context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[E] =
    new BoundedLatenessGenerator[E](maxAllowedLateness, streamID)
}
