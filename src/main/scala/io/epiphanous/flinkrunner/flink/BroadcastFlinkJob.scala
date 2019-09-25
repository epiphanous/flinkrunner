package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.SEE
import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkEvent}
import io.epiphanous.flinkrunner.util.StreamUtils._
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{BroadcastConnectedStream, DataStream}

/**
  * A job using the <a href="https://flink.apache.org/2019/06/26/broadcast-state.html">broadcast stream join pattern</a>.
  * @tparam IN Input stream event type
  * @tparam BC Broadcast stream event type
  * @tparam OUT  Output stream event type
  */
abstract class BroadcastFlinkJob[
  IN <: FlinkEvent: TypeInformation,
  BC <: FlinkEvent: TypeInformation,
  OUT <: FlinkEvent: TypeInformation]
    extends BaseFlinkJob[BroadcastConnectedStream[IN, BC], OUT] {

  import BroadcastFlinkJob._

  /**
    * Instantiate a keyed broadcast process function used to produce an output data stream from the connected
    * broadcast + events stream. Must be overridden by sub-classes.
    * @param config implicit flink config
    * @return KeyedBroadcastProcessFunction[String, IN, BC, OUT]
    */
  def getBroadcastProcessFunction()(implicit config: FlinkConfig): KeyedBroadcastProcessFunction[String, IN, BC, OUT]

  /**
    * Creates the broadcast state descriptor.
    * @param nameOpt the name of the broadcast stream in the source configuration (default "broadcast")
    * @param config implicit flink config
    * @return MapStateDescriptor[String, BC]
    */
  def getBroadcastStateDescriptor(
    nameOpt: Option[String] = None
  )(implicit config: FlinkConfig
  ): MapStateDescriptor[String, BC] =
    new MapStateDescriptor[String, BC](nameOpt.getOrElse(BROADCAST_STATE_DESCRIPTOR_NAME),
                                       createTypeInformation[String],
                                       createTypeInformation[BC])

  /**
    * Creates the broadcast source stream.
    * @param config implicit flink config
    * @param env implicit streaming execution environment
    * @return broadcast stream
    */
  def broadcastSource(implicit config: FlinkConfig, env: SEE): BroadcastStream[BC] =
    fromSource[BC](getBroadcastSourceName).broadcast(getBroadcastStateDescriptor())

  def getBroadcastSourceName()(implicit config: FlinkConfig) = BROADCAST_SOURCE_NAME
  def getEventSourceName()(implicit config: FlinkConfig) = EVENT_SOURCE_NAME

  /**
    * Creates the broadcast stream and the input event stream and connects them
    * @param config implicit flink config
    * @param env implicit streaming execution environment
    * @return connected broadcast + events stream
    */
  override def source()(implicit config: FlinkConfig, env: SEE): BroadcastConnectedStream[IN, BC] =
    (fromSource[IN](getEventSourceName) |> maybeAssignTimestampsAndWatermarks[IN])
      .keyBy(in => in.$key)
      .connect(broadcastSource)

  /**
    * Applies the process function returned by getBroadcastProcessFunction to the connected stream (broadcast + events)
    * created by source().
    *
    * @param in connected broadcast + data stream created by source()
    * @param config implicit flink job config
    * @param env implicit streaming execution environment
    * @return output data stream
    */
  override def transform(
    in: BroadcastConnectedStream[IN, BC]
  )(implicit config: FlinkConfig,
    env: SEE
  ): DataStream[OUT] = {
    val name = s"processed:${getEventSourceName()}+${getBroadcastSourceName()}"
    in.process(getBroadcastProcessFunction()).name(name).uid(name)
  }

}

object BroadcastFlinkJob {
  final val BROADCAST_SOURCE_NAME = "broadcast"
  final val EVENT_SOURCE_NAME = "events"
  final val BROADCAST_STATE_DESCRIPTOR_NAME = "broadcast state"
}
