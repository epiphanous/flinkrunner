package io.epiphanous.flinkrunner.util

import io.epiphanous.flinkrunner.model.FlinkEvent
import org.apache.flink.api.common.eventtime.{WatermarkGeneratorSupplier, WatermarkStrategy}

class BoundedLatenessWatermarkStrategy[E <: FlinkEvent](
    maxAllowedLateness: Long,
    streamID: String)
    extends WatermarkStrategy[E] {
  override def createWatermarkGenerator(
      context: WatermarkGeneratorSupplier.Context) =
    new BoundedLatenessGenerator(maxAllowedLateness, streamID)
}
