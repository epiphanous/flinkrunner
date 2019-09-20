package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.SEE
import io.epiphanous.flinkrunner.model.{DataOrControl, FlinkConfig, FlinkEvent}
import io.epiphanous.flinkrunner.util.StreamUtils._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream

/**
  * A simple flink job that interleaves a data stream and a control stream and creates a source of
  * data records that occur during active control periods.
  *
  * As an example, let `on` represent an active control, `off` represent an inactive control,
  * and `d` to represent data elements. Then the following stream:
  *
  *    `d1 d2 on d3 d4 d5 off d6 d7`
  *
  * would output `d3 d4 d5`.
  *
  * @tparam D   the data type
  * @tparam C   the control type
  * @tparam OUT the output stream element type
  */
abstract class FilterByControlJob[
  D <: FlinkEvent: TypeInformation,
  C <: FlinkEvent: TypeInformation,
  OUT <: FlinkEvent: TypeInformation]
    extends FlinkJob[D, OUT] {

  /**
    * A source data stream for the data events.
    * @param config implicit flink config
    * @return a data stream of data events.
    */
  def data(implicit config: FlinkConfig, env: SEE): DataStream[D] = fromSource[D]("data")

  /**
    * A source data stream for the control events.
    * @param config implicit flink config
    * @return a data stream of control events.
    */
  def control(implicit config: FlinkConfig, env: SEE): DataStream[C] = fromSource[C]("control")

  /**
    * Generate a stream of data records filtered by the control stream.
    * This method does not generally need to be overridden
    * in subclasses. It interleaves the data and control streams to produce a single stream of
    * [[DataOrControl]] objects and then uses a flat map with state to determine when to emit
    * the data records. It remembers the last control time and state and updates it when the state changes.
    **
    * @param config implicit flink config
    * @return data stream of data records
    */
  override def source()(implicit config: FlinkConfig, env: SEE): DataStream[D] = {

    val controlLockoutDuration = config.getDuration("control.lockout.duration").toMillis

    val in = maybeAssignTimestampsAndWatermarks(
      data
        .connect(control)
        .map(DataOrControl.data[D, C], DataOrControl.control[D, C])
        .name("data+control")
        .uid("data+control")
    )

    in.keyBy((e: DataOrControl[D, C]) => e.$key)
      .filterWithState[(Long, Boolean)]((dc, lastControlOpt) => {
        if (dc.isData) {
          val emit = lastControlOpt match {
            case Some((ts: Long, active: Boolean)) =>
              active && ((dc.$timestamp - ts) >= controlLockoutDuration)
            case None => false
          }
          (emit, lastControlOpt)
        } else {
          val update = lastControlOpt match {
            case Some((_, active)) => dc.$active != active
            case None              => true
          }
          (false, if (update) Some((dc.$timestamp, dc.$active)) else lastControlOpt)
        }
      })
      .name(s"filter:${in.name}")
      .uid(s"filter:${in.name}")
      .map(_.data.get)
      .name("filtered:data")
      .uid("filtered:data")
  }

}
