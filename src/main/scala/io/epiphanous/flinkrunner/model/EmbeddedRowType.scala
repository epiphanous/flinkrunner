package io.epiphanous.flinkrunner.model

import org.apache.flink.types.Row

trait EmbeddedRowType {
  def toRow: Row
}
