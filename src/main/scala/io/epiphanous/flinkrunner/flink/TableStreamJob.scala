package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model.{EmbeddedRowType, FlinkEvent}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream

import scala.reflect.runtime.{universe => ru}

/** A job class to generate streaming output tables.
  * @param runner
  *   an instance of [[FlinkRunner]]
  * @tparam OUT
  *   the output type
  * @tparam ADT
  *   the algebraic data type of the [[FlinkRunner]] instance
  */
abstract class TableStreamJob[
    OUT <: ADT with EmbeddedRowType: TypeInformation: ru.TypeTag,
    ADT <: FlinkEvent: TypeInformation](runner: FlinkRunner[ADT])
    extends StreamJob[OUT, ADT](runner) {

  override def sink(out: DataStream[OUT]): Unit = {
    runner.mainSinkConfigs.foreach(_.addRowSink[OUT](out))
    if (runner.sideSinkConfigs.nonEmpty) sinkSideOutputs(out)
  }
}
