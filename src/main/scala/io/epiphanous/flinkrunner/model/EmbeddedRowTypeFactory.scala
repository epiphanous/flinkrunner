package io.epiphanous.flinkrunner.model

import org.apache.flink.table.data.RowData

/** Companion objects of event types that wrap row-based records should
  * implement this trait to support row deserialization. A companion trait,
  * EmbeddedRowType, can be used to support serializing row records from
  * the flink events that implement it.
  * @tparam E
  *   a flink event that implements EmbeddedRowType
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
