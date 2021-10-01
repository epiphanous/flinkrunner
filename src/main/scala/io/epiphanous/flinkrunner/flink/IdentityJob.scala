package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model.FlinkEvent
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream

/**
 * An identity mapper that passes through an input events to its configured
 * sinks.
 * @param runner
 *   the flink runner associated with this job
 * @tparam E
 *   the input and output event type
 * @tparam ADT
 *   The flink runner's algebraic data type
 */
class IdentityJob[
    E <: ADT: TypeInformation,
    ADT <: FlinkEvent: TypeInformation](runner: FlinkRunner[ADT])
    extends FlinkJob[E, E, ADT](runner) {

  /**
   * Does the identity transform (passes the stream through unchanged).
   *
   * @param in
   *   input data stream created by source()
   * @return
   *   output data stream
   */
  override def transform(in: DataStream[E]): DataStream[E] = in
}
