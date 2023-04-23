package io.epiphanous.flinkrunner.model

import org.apache.flink.table.data.RowData

/** Companion objects of event types that implement EmbeddedRowType and
  * require Row-based sources should implement this trait to support row
  * deserialization.
  * @tparam E
  *   a flink event type that implements EmbeddedRowType
  */
trait EmbeddedRowTypeFactory[E <: FlinkEvent with EmbeddedRowType] {

  /** Construct an event of type E from row data.
    *
    * @param rowData
    *   row data to convert to event of type E
    * @return
    *   New event of type E
    */
  implicit def fromRowData(rowData: RowData): E
}
