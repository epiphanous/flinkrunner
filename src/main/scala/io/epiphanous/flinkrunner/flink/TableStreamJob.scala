package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model.{EmbeddedRowType, FlinkEvent}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.types.logical.RowType

/** A job class to generate streaming output tables.
  * @param runner
  *   an instance of [[FlinkRunner]]
  * @tparam OUT
  *   the output type
  * @tparam ADT
  *   the algebraic data type of the [[FlinkRunner]] instance
  */
abstract class TableStreamJob[
    OUT <: ADT with EmbeddedRowType,
    ADT <: FlinkEvent](runner: FlinkRunner[ADT])
    extends StreamJob[OUT, ADT](runner) {

  def getRowType: Option[RowType] = None

  override def sink(out: DataStream[OUT]): Unit =
    runner.getSinkNames.foreach(name =>
      runner.addRowSink[OUT](out, name, getRowType)
    )
}
