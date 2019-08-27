package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.SEE
import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkEvent}
import org.apache.flink.api.common.typeinfo.TypeInformation
import io.epiphanous.flinkrunner.util.StreamUtils._
import org.apache.flink.api.scala._
import org.apache.flink.api.common.state.{MapStateDescriptor, StateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{BroadcastConnectedStream, DataStream}

/**
  * A job using the <a href="https://flink.apache.org/2019/06/26/broadcast-state.html">broadcast stream join pattern</a>.
  * @param processFunction KeyedBroadcastProcessFunction
  * @tparam BC Broadcast stream event type
  * @tparam IN Input stream event type
  * @tparam OUT  Output stream event type
  */
abstract class BroadcastFlinkJob[
  BC <: FlinkEvent: TypeInformation,
  IN <: FlinkEvent: TypeInformation,
  OUT <: FlinkEvent: TypeInformation
](processFunction: KeyedBroadcastProcessFunction[String, IN, BC, OUT])
    extends BaseFlinkJob[BroadcastConnectedStream[IN, BC], OUT] {

  import BroadcastFlinkJob._

  def getBroadcastStateDescriptor()(implicit config: FlinkConfig): MapStateDescriptor[String, BC]

  def broadcastSource(implicit config: FlinkConfig, env: SEE): BroadcastStream[BC] =
    fromSource[BC](getBroadcastSourceName).broadcast(getBroadcastStateDescriptor)

  def getBroadcastSourceName()(implicit config: FlinkConfig) = BROADCAST_SOURCE_NAME
  def getEventSourceName()(implicit config: FlinkConfig) = EVENT_SOURCE_NAME

  override def source()(implicit config: FlinkConfig, env: SEE): BroadcastConnectedStream[IN, BC] =
    (fromSource[IN](getEventSourceName) |# maybeAssignTimestampsAndWatermarks)
      .keyBy(in => in.$key)
      .connect(broadcastSource)

  def maybeAssignTimestampsAndWatermarks(in: DataStream[IN])(implicit config: FlinkConfig, env: SEE): Unit =
    if (env.getStreamTimeCharacteristic == TimeCharacteristic.EventTime)
      in.assignTimestampsAndWatermarks(boundedLatenessEventTime[IN]())

  override def transform(
    in: BroadcastConnectedStream[IN, BC]
  )(implicit config: FlinkConfig,
    env: SEE
  ): DataStream[OUT] =
    in.process(processFunction)

}

object BroadcastFlinkJob {
  final val BROADCAST_SOURCE_NAME = "broadcast"
  final val EVENT_SOURCE_NAME = "events"
}
