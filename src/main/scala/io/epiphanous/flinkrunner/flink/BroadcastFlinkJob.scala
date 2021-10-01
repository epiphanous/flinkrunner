package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model.FlinkEvent
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{
  BroadcastConnectedStream,
  DataStream
}

/**
 * A job using the <a
 * href="https://flink.apache.org/2019/06/26/broadcast-state.html">broadcast
 * stream join pattern</a>.
 *
 * @param runner
 *   the flink runner associated with this job
 * @tparam IN
 *   Input stream event type
 * @tparam BC
 *   Broadcast stream event type
 * @tparam OUT
 *   Output stream event type
 * @tparam ADT
 *   The flink runner's algebraic data type
 */
abstract class BroadcastFlinkJob[
    IN <: ADT: TypeInformation,
    BC <: ADT: TypeInformation,
    OUT <: ADT: TypeInformation,
    ADT <: FlinkEvent: TypeInformation](runner: FlinkRunner[ADT])
    extends BaseFlinkJob[BroadcastConnectedStream[IN, BC], OUT, ADT](
      runner
    ) {

  import BroadcastFlinkJob._

  /**
   * Instantiate a keyed broadcast process function used to produce an
   * output data stream from the connected broadcast + events stream. Must
   * be overridden by sub-classes.
   *
   * @return
   *   KeyedBroadcastProcessFunction[String, IN, BC, OUT]
   */
  def getBroadcastProcessFunction
      : KeyedBroadcastProcessFunction[String, IN, BC, OUT]

  /**
   * Creates the broadcast state descriptor.
   *
   * @param nameOpt
   *   the name of the broadcast stream in the source configuration
   *   (default "broadcast")
   * @return
   *   MapStateDescriptor[String, BC]
   */
  def getBroadcastStateDescriptor(
      nameOpt: Option[String] = None
  ): MapStateDescriptor[String, BC] =
    new MapStateDescriptor[String, BC](
      nameOpt.getOrElse(BROADCAST_STATE_DESCRIPTOR_NAME),
      createTypeInformation[String],
      createTypeInformation[BC]
    )

  /**
   * Creates the broadcast source stream.
   *
   * @return
   *   broadcast stream
   */
  def broadcastSource: BroadcastStream[BC] =
    runner
      .fromSource[BC](getBroadcastSourceName)
      .broadcast(
        getBroadcastStateDescriptor()
      )

  /**
   * Creates the broadcast stream and the input event stream and connects
   * them
   *
   * @return
   *   connected broadcast + events stream
   */
  override def source(): BroadcastConnectedStream[IN, BC] =
    (runner
      .fromSource[IN](getEventSourceName))
      .keyBy((in: IN) => in.$key)
      .connect(broadcastSource)

  /**
   * Applies the process function returned by getBroadcastProcessFunction
   * to the connected stream (broadcast + events) created by source().
   *
   * @param in
   *   connected broadcast + data stream created by source()
   * @param config
   *   implicit flink job config
   * @param env
   *   implicit streaming execution environment
   * @return
   *   output data stream
   */
  override def transform(
      in: BroadcastConnectedStream[IN, BC]
  ): DataStream[OUT] = {
    val name =
      s"processed:$getEventSourceName+$getBroadcastSourceName"
    in.process(getBroadcastProcessFunction).name(name).uid(name)
  }

  def getBroadcastSourceName: String = BROADCAST_SOURCE_NAME

  def getEventSourceName: String = EVENT_SOURCE_NAME

}

object BroadcastFlinkJob {
  final val BROADCAST_SOURCE_NAME           = "broadcast"
  final val EVENT_SOURCE_NAME               = "events"
  final val BROADCAST_STATE_DESCRIPTOR_NAME = "broadcast state"
}
