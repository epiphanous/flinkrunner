package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkConnectorName,
  FlinkEvent
}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{
  DataStream,
  StreamExecutionEnvironment
}

case class CollectionSourceConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig,
    connector: FlinkConnectorName = FlinkConnectorName.Collection)
    extends SourceConfig[ADT] {
  val topic: String = name

  def getSource[E <: ADT: TypeInformation](
      env: StreamExecutionEnvironment,
      mockSources: Map[String, Seq[ADT]]): DataStream[E] =
    env
      .fromCollection(
        mockSources
          .getOrElse(
            topic,
            throw new RuntimeException(s"missing collection source $topic")
          )
          .asInstanceOf[Seq[E]]
      )
      .name(s"raw:$label")
      .uid(s"raw:$label")
}
