package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model.FlinkEvent
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream

/**
 * An abstract flink job to transform on a stream of events from an
 * algebraic data type (ADT).
 *
 * @param runner
 *   the flink runner associated with this job
 * @tparam IN
 *   The type of input stream elements
 * @tparam OUT
 *   The type of output stream elements
 * @tparam ADT
 *   The flink runner's algebraic data type
 */
abstract class FlinkJob[
    IN <: ADT: TypeInformation,
    OUT <: ADT: TypeInformation,
    ADT <: FlinkEvent: TypeInformation](runner: FlinkRunner[ADT])
    extends BaseFlinkJob[DataStream[IN], OUT, ADT](runner) {

  /**
   * Return the primary event source name
   * @return
   *   primary source name
   */
  def getEventSourceName: String =
    config.getSourceNames.headOption.getOrElse("events")

  /**
   * Returns source data stream to pass into transform(). This can be
   * overridden by subclasses.
   *
   * @return
   *   input data stream
   */
  def source(): DataStream[IN] =
    runner.fromSource[IN](getEventSourceName)

}
