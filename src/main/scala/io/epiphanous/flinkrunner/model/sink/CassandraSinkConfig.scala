package io.epiphanous.flinkrunner.model.sink

import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkConnectorName, FlinkEvent}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.cassandra.CassandraSink

case class CassandraSinkConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig,
    connector: FlinkConnectorName = FlinkConnectorName.CassandraSink
) extends SinkConfig[ADT] {

  val host: String  = config.getString(pfx("host"))
  val query: String = config.getString(pfx("query"))

  def getSink[E <: ADT](stream: DataStream[E]): CassandraSink[E] =
    CassandraSink
      .addSink(stream)
      .setHost(host)
      .setQuery(query)
      .build()
      .uid(label)
      .name(label)
}
