package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.model.{DataOrControl, FlinkEvent}
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
  * @param sources     not used
  * @param mixedSource for testing, provides a sequence of DataOrControl[D,C] objects
  * @tparam D   the data type
  * @tparam C   the control type
  * @tparam OUT the output stream element type
  */
abstract class FilterByControlJob[D <: FlinkEvent: TypeInformation,
                                  C <: FlinkEvent: TypeInformation,
                                  OUT <: FlinkEvent: TypeInformation](
//    helper: DataOrControlEventHelper[D, C],
    sources: Map[String, Seq[Array[Byte]]] = Map.empty,
    mixedSource: Seq[DataOrControl[D, C]] = Seq.empty)
    extends SimpleFlinkJob[D, OUT](sources) {

  /**
    * A source data stream for the data events. Must be overridden by subclasses.
    * @param args implicit flink args
    * @param env implicit flink execution environment
    * @return a data stream of data events.
    */
  def data(implicit args: Args, env: SEE): DataStream[D]

  /**
    * A source data stream for the control events.  Must be overridden by subclasses.
    * @param args implicit flink args
    * @param env implicit flink execution environment
    * @return a data stream of control events.
    */
  def control(implicit args: Args, env: SEE): DataStream[C]

  /**
    * Generate a stream of data records filtered by the control stream.
    * This method does not generally need to be overridden
    * in subclasses. It interleaves the data and control streams to produce a single stream of
    * [[DataOrControl]] objects and then uses a flat map with state to determine when to emit
    * the data records. It remembers the last control time and state and updates it when the state changes.
    **
    * @param args implicit flink args
    * @param env implicit flink execution environment
    * @return data stream of data records
    */
  override def source()(implicit args: Args, env: SEE): DataStream[D] = {

    val controlLockoutDuration = args.getLong("control.lockout.duration")

    val in =
      if (mixedSource.nonEmpty)
        env.fromCollection(mixedSource)
      else
        data
          .connect(control)
          .map(DataOrControl.data[D, C], DataOrControl.control[D, C])
          .keyBy(_.$key)

    in.assignTimestampsAndWatermarks(boundedLatenessEventTime[DataOrControl[D, C]]())
      .keyBy(_.$key)
      .filterWithState[(Long, Boolean)]((dc, lastControlOpt) => {
        if (dc.isData) {
          val emit = lastControlOpt match {
            case Some((ts, active)) =>
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
      .map(_.data.get)
  }

}
