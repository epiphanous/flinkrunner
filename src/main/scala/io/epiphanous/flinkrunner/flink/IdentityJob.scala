package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.SEE
import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkEvent}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream

class IdentityJob[E <: FlinkEvent: TypeInformation]
    extends FlinkJob[E, E] {

  /**
   * Does the identity transform (passes the stream through unchanged).
   *
   * @param in
   *   input data stream created by source()
   * @param config
   *   implicit flink job config
   * @param env
   *   streaming execution environment
   * @return
   *   output data stream
   */
  override def transform(
      in: DataStream[E])(implicit config: FlinkConfig, env: SEE) =
    in
}
