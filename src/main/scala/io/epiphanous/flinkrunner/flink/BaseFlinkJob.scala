package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model.FlinkEvent
import io.epiphanous.flinkrunner.util.StreamUtils.Pipe
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream

/**
 * An abstract flink job to transform on an input stream into an output
 * stream.
 * @param runner
 *   the flink runner associated with this job
 * @tparam DS
 *   The type of the input data stream (not its elements)
 * @tparam OUT
 *   The type of output stream elements
 * @tparam ADT
 *   The flink runner's algebraic data type
 */
abstract class BaseFlinkJob[
    DS: TypeInformation,
    OUT <: ADT: TypeInformation,
    ADT <: FlinkEvent: TypeInformation](runner: FlinkRunner[ADT])
    extends StreamJob[OUT, ADT](runner) {

  /**
   * A pipeline for transforming a single stream. Passes the output of
   * source() through transform() and the result of that into maybeSink(),
   * which may pass it into sink() if we're not testing. Ultimately,
   * returns the output data stream to facilitate testing.
   *
   * @return
   *   data output stream
   */
  def flow(): DataStream[OUT] = source |> transform |# maybeSink

  /**
   * Returns source data stream to pass into transform(). This must be
   * overridden by subclasses.
   *
   * @return
   *   input data stream
   */
  def source(): DS

  /**
   * Primary method to transform the source data stream into the output
   * data stream. The output of this method is passed into sink(). This
   * method must be overridden by subclasses.
   *
   * @param in
   *   input data stream created by source()
   * @return
   *   output data stream
   */
  def transform(in: DS): DataStream[OUT]

}
