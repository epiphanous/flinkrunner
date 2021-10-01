package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model.{DataOrControl, FlinkEvent}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream

/**
 * A simple flink job that interleaves a data stream and a control stream
 * and creates a source of data records that occur during active control
 * periods.
 *
 * As an example, let `on` represent an active control, `off` represent an
 * inactive control, and `d` to represent data elements. Then the following
 * stream:
 *
 * `d1 d2 on d3 d4 d5 off d6 d7`
 *
 * would output `d3 d4 d5`.
 *
 * @param runner
 *   the flink runner associated with this job
 * @tparam D
 *   the data type
 * @tparam C
 *   the control type
 * @tparam OUT
 *   the output stream element type
 */
abstract class FilterByControlJob[
    D <: ADT: TypeInformation,
    C <: ADT: TypeInformation,
    OUT <: ADT: TypeInformation,
    ADT <: FlinkEvent: TypeInformation](runner: FlinkRunner[ADT])
    extends FlinkJob[D, OUT, ADT](runner) {

  import io.epiphanous.flinkrunner.flink.FilterByControlJob._

  /**
   * A source data stream for the data events.
   *
   * @return
   *   a data stream of data events.
   */
  def data: DataStream[D] =
    runner.fromSource[D](getDataStreamName)

  /**
   * A source data stream for the control events.
   *
   * @return
   *   a data stream of control events.
   */
  def control: DataStream[C] = runner.fromSource[C](getControlStreamName)

  /**
   * Generate a stream of data records filtered by the control stream. This
   * method does not generally need to be overridden in subclasses. It
   * interleaves the data and control streams to produce a single stream of
   * DataOrControl objects and then uses a flat map with state to determine
   * when to emit the data records. It remembers the last control time and
   * state and updates it when the state changes. *
   *
   * @return
   *   data stream of data records
   */
  override def source(): DataStream[D] = {

    val controlLockoutDuration =
      config.getDuration("control.lockout.duration").toMillis

    val name = getDataControlStreamName

    implicit val typeInformation
        : TypeInformation[DataOrControl[D, C, ADT]] =
      TypeInformation.of(classOf[DataOrControl[D, C, ADT]])

    val in =
      data
        .connect(control)
        .map(
          DataOrControl.data[D, C, ADT],
          DataOrControl.control[D, C, ADT]
        )
        .name(name)
        .uid(name)

    in.keyBy((e: DataOrControl[D, C, ADT]) => e.$key)
      .filterWithState[(Long, Boolean)]((dc, lastControlOpt) => {
        if (dc.isData) {
          val emit = lastControlOpt match {
            case Some((ts: Long, active: Boolean)) =>
              active && ((dc.$timestamp - ts) >= controlLockoutDuration)
            case None                              => false
          }
          (emit, lastControlOpt)
        } else {
          val update = lastControlOpt match {
            case Some((_, active)) => dc.$active != active
            case None              => true
          }
          (
            false,
            if (update) Some((dc.$timestamp, dc.$active))
            else lastControlOpt
          )
        }
      })
      .name(s"filter:${in.name}")
      .uid(s"filter:${in.name}")
      .map((x: DataOrControl[D, C, ADT]) => x.data.get)
      .name("filtered:data")
      .uid("filtered:data")
  }

  def getDataStreamName: String = DATA_STREAM_NAME

  def getControlStreamName: String = CONTROL_STREAM_NAME

  def getDataControlStreamName: String =
    s"$getDataStreamName+$getControlStreamName"

}

object FilterByControlJob {
  val DATA_STREAM_NAME    = "data"
  val CONTROL_STREAM_NAME = "control"
}
