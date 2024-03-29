package io.epiphanous.flinkrunner.model.sink

import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.util.AvroUtils.RichGenericRecord
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.cassandra._
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.types.Row

/** A cassandra sink config.
  *
  * Configuration:
  *
  *   - `host`: the cassandra endpoint
  *   - `query`: an insert query
  *
  * @param name
  *   name of the sink
  * @param config
  *   flink runner configuration
  * @tparam ADT
  *   the flinkrunner algebraic data type
  */
case class CassandraSinkConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig
) extends SinkConfig[ADT] {

  override val connector: FlinkConnectorName =
    FlinkConnectorName.Cassandra

  val host: String  =
    config.getStringOpt(pfx("host")).getOrElse("localhost")
  val port: Int     = config.getIntOpt(pfx("port")).getOrElse(9042)
  val query: String = config.getString(pfx("query"))

  val clusterBuilder = new CassandraClusterBuilder(host, port)

  def addSink[E <: ADT: TypeInformation](stream: DataStream[E]): Unit = {
    CassandraSink
      .addSink(stream)
      .setClusterBuilder(clusterBuilder)
      .setQuery(query)
      .build()
    ()
  }

  override def addAvroSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](stream: DataStream[E]): Unit = {
    stream
      .addSink(
        new AbstractCassandraTupleSink[E](
          query,
          clusterBuilder,
          CassandraSinkBaseConfig.newBuilder().build(),
          new NoOpCassandraFailureHandler()
        ) {
          override def extract(record: E): Array[AnyRef] =
            record.$record.getDataAsSeq.toArray
        }
      )
      .uid(label)
      .name(label)
    ()
  }

  override def _addRowSink(
      stream: DataStream[Row],
      rowType: RowType): Unit = {
    CassandraSink
      .addSink(stream)
      .setClusterBuilder(clusterBuilder)
      .setQuery(query)
      .build()
    ()
  }
}
