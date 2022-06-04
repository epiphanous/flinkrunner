package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkConnectorName,
  FlinkEvent
}
import io.epiphanous.flinkrunner.serde.JsonKinesisDeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{
  DataStream,
  StreamExecutionEnvironment
}
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema

case class KinesisSourceConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig,
    connector: FlinkConnectorName = FlinkConnectorName.Kinesis)
    extends SourceConfig[ADT] {

  val stream: String = config.getString(pfx("stream"))

  /** Returns a deserialization schema for kinesis. This implementation
    * assumes JSON formatted event records.
    * @tparam E
    *   the event type
    * @return
    *   KinesisDeserializationSchema[E]
    */
  def getDeserializationSchema[E <: ADT: TypeInformation]
      : KinesisDeserializationSchema[E] =
    new JsonKinesisDeserializationSchema[E, ADT](this)

  def getSource[E <: ADT: TypeInformation](
      env: StreamExecutionEnvironment): DataStream[E] = {
    env
      .addSource(
        new FlinkKinesisConsumer[E](
          stream,
          getDeserializationSchema[E],
          properties
        )
      )
      .assignTimestampsAndWatermarks(
        getWatermarkStrategy[E]
      )
      .name(s"wm:$label")
      .uid(s"wm:$label")
  }
}
