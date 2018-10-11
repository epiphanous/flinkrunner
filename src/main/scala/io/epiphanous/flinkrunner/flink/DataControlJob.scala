package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.util.StreamUtils._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * A simple flink job that transforms a data stream and a control stream into an output stream. This
  * uses flink's CEP library to match sequences of data elements that fall between control elements
  * that are alternately active and inactive. As an example, let `on` represent an active control,
  * `off` represent an inactive control, and `d` to represent data elements. Then the following stream
  * `d1 d2 on d3 d4 d5 off d6 d7` would output one [[DataControlPeriod]] object with a start time of
  * the timestamp of `on`, and end time of the timestamp of `off` and the elements `d3, d4, d5` as the
  * payload.
  *
  * @param sources     not used
  * @param mixedSource for testing, provides a sequence of DataOrControl[D,C] objects
  * @tparam D   the data type
  * @tparam C   the control type
  * @tparam OUT the output stream element type
  */
abstract class DataControlJob[
    D <: FlinkEvent: TypeInformation,
    C <: FlinkEvent: TypeInformation,
    OUT <: FlinkEvent: TypeInformation
  ](//    helper: DataOrControlEventHelper[D, C],
    sources: Map[String, Seq[Array[Byte]]] = Map.empty,
    mixedSource: Seq[DataOrControl[D, C]] = Seq.empty)
    extends SimpleFlinkJob[DataControlPeriod[D], OUT](sources) {

  import DataControlJob._

  /**
    * A source data stream for the data events. Must be overridden by subclasses.
    * @param args implicit flink args
    * @param env implicit flink execution environment
    * @return a data stream of data events.
    */
  def data(implicit args: Args, env: SEE): DataStream[D] = fromSource[D](sources, "data")

  /**
    * A source data stream for the control events.  Must be overridden by subclasses.
    * @param args implicit flink args
    * @param env implicit flink execution environment
    * @return a data stream of control events.
    */
  def control(implicit args: Args, env: SEE): DataStream[C] = fromSource[C](sources, "control")

  /**
    * Generate a data stream of data control periods. This method does not generally need to be overridden
    * in subclasses. It interleaves the data and control streams to produce a single stream of
    * [[DataOrControl]] objects and then uses flink's CEP library to match sequences of control-on, data, and
    * control-off events, which aggregates into a stream of [[DataControlPeriod]]s.
    *
    * At the moment, flink's CEP library has a bug in how its `greedy` operator works, which requires that
    * we manually filter the control stream to remove multiple, sequential controls with the same `isActive()`
    * value. So multiple `on` controls are replaced with the earliest `on` control and the same for `off` controls.
    *
    * @param args implicit flink args
    * @param env implicit flink execution environment
    * @return data stream of data control periods
    */
  override def source()(implicit args: Args, env: SEE): DataStream[DataControlPeriod[D]] = {

    val controlLockoutDuration = args.getLong("control.lockout.duration")

    val in =
      if (mixedSource.nonEmpty)
        env.fromCollection(mixedSource)
      else
        data
          .connect(control)
          .map(DataOrControl.data[D, C], DataOrControl.control[D, C])

    val keyed = in
      .assignTimestampsAndWatermarks(boundedLatenessEventTime[DataOrControl[D, C]]())
      .keyBy(_.$key)
      // this is here to debounce multiple controls because of a bug in flink's CEP pattern
      // matching where the greedy operator doesn't work properly
      // (see https://issues.apache.org/jira/browse/FLINK-8914)
      .filterWithState[Boolean]((dc, lastControlOpt) => {
        if (dc.isData) (true, lastControlOpt)
        else {
          val dcIsActive = dc.$active
          lastControlOpt match {
            case Some(lastControl) =>
              if (dcIsActive != lastControl)
                (true, Some(dcIsActive))
              else
                (false, lastControlOpt)
            case None => (true, Some(dcIsActive))
          }
        }
      })
      .keyBy(_.$key)

    val debug = args.debug
    CEP
      .pattern(keyed, pattern)
      .flatSelect((pat, out) => {
        if (debug)
          println(
            s"\n*** MATCHED ***\n${pat.map(kv => s"${kv._1} => ${kv._2.map(_.info).mkString("; ")}").mkString("\n  - ")}\n")
        try {
          val on = pat(CEP_CONTROL_ON).head.control.get
          val off = pat(CEP_CONTROL_OFF).head.control.get
          val elements = pat(CEP_ACTIVE).toList
            .filter(_.isData)
            .map(_.data.get)
            .filter(_.$timestamp - on.$timestamp >= controlLockoutDuration)
          if (elements.nonEmpty)
            out.collect(
              DataControlPeriod[D](key = on.$key, start = on.$timestamp, end = off.$timestamp, elements = elements))
        } catch {
          case e: Exception =>
            println(s"\n*** MATCH ERROR***\n$e\n")
        }
      })

  }

  /**
    * Returns the pattern used by CEP to aggregate the control and data streams. Should not need to be overridden.
    * @return cep pattern
    */
  def pattern(implicit args: Args): Pattern[DataOrControl[D, C], DataOrControl[D, C]] = {

    val maxActiveDuration = Time.seconds(args.getLong("max.active.duration"))
    val debug = args.debug
    val condition =
      (dc: DataOrControl[D, C], f: DataOrControl[D, C] => Boolean, name: String) => {
        val x = f(dc)
        if (debug) println(s"$name: $x ${dc.info}")
        x
      }
    val activeCondition = (el: DataOrControl[D, C]) =>
      condition(el, dc => (dc.isControl && dc.$active) || dc.isData, CEP_ACTIVE)
    val inactiveCondition = (el: DataOrControl[D, C]) =>
      condition(el, dc => (dc.isControl && !dc.$active) || dc.isData, CEP_INACTIVE)
    val controlOnCondition = (el: DataOrControl[D, C]) =>
      condition(el, dc => dc.isControl && dc.$active, CEP_CONTROL_ON)
    val controlOffCondition = (el: DataOrControl[D, C]) =>
      condition(el, dc => dc.isControl && !dc.$active, CEP_CONTROL_OFF)

    // active pattern
    val skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent()
    val active = Pattern
      .begin[DataOrControl[D, C]](CEP_ACTIVE, skipStrategy)
      .where(activeCondition)
      .oneOrMore
      .consecutive()
      .greedy // not working: see https://issues.apache.org/jira/browse/FLINK-8914

    // inactive pattern
    val inactive = Pattern
      .begin[DataOrControl[D, C]](CEP_INACTIVE, skipStrategy)
      .where(inactiveCondition)
      .oneOrMore
      .consecutive()
      .greedy

    // pattern is:
    //   control-on immediately followed by
    //   active immediately followed by
    //   control-off
    //   optionally immediately followed by
    //   inactive
    // within [[maxActiveDuration]] seconds
    Pattern
      .begin[DataOrControl[D, C]](CEP_CONTROL_ON, skipStrategy)
      .where(controlOnCondition)
      .next(active)
      .next(CEP_CONTROL_OFF)
      .where(controlOffCondition)
      .next(inactive)
      .optional
      .within(maxActiveDuration)

  }
}

object DataControlJob {
  val CEP_CONTROL_ON = "control-on"
  val CEP_CONTROL_OFF = "control-off"
  val CEP_ACTIVE = "active"
  val CEP_INACTIVE = "inactive"
}
