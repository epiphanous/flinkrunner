package io.epiphanous.flinkrunner.model

import org.apache.flink.types.{Row, RowKind}

/** Event types that are used in TableStreamJobs should implement this
  * trait. This trait endows event types with a `toRow` method, to convert
  * the underlying event type to an instance of a flink [[Row]] object.
  */
trait EmbeddedRowType {
  def toRow: Row

  def rowKind: RowKind = RowKind.INSERT
}
