package io.epiphanous.flinkrunner.model.sink

import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model.{FlinkConnectorName, FlinkEvent}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.cassandra.CassandraSink

/** A cassandra sink config.
  *
  * Configuration:
  *
  *   - `host`: the cassandra endpoint
  *   - `query`: an insert query
  *
  * @param name
  *   name of the sink
  * @param runner
  *   flink runner instance
  * @tparam ADT
  *   the flinkrunner algebraic data type
  */
case class CassandraSinkConfig[ADT <: FlinkEvent](
    name: String,
    runner: FlinkRunner[ADT]
) extends SinkConfig[ADT] {

  override val connector: FlinkConnectorName =
    FlinkConnectorName.CassandraSink

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
