package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.SEE
import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkEvent}
import io.epiphanous.flinkrunner.util.StreamUtils._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream

/**
  * An abstract flink job to transform on a stream of events from an algebraic data type (ADT).
  *
  * @tparam IN  The type of input stream elements
  * @tparam OUT The type of output stream elements
  */
abstract class FlinkJob[IN <: FlinkEvent : TypeInformation, OUT <: FlinkEvent : TypeInformation]
  extends BaseFlinkJob[DataStream[IN], OUT] {

  def getEventSourceName(implicit config: FlinkConfig) = config.getSourceNames.headOption.getOrElse("events")

  /**
    * Returns source data stream to pass into transform(). This can be overridden by subclasses.
    *
    * @return input data stream
    */
  def source()(implicit config: FlinkConfig, env: SEE): DataStream[IN] =
    fromSource[IN](getEventSourceName) |> maybeAssignTimestampsAndWatermarks[IN]

}
