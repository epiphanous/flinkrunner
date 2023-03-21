package io.epiphanous.flinkrunner.model.sink

import com.datastax.driver.core.{Cluster, CodecRegistry}
import com.datastax.driver.extras.codecs.jdk8.InstantCodec
import io.epiphanous.flinkrunner.model.{
  EmbeddedAvroRecord,
  FlinkConfig,
  FlinkConnectorName,
  FlinkEvent
}
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

  /** Don't convert to single abstract method...flink will complain
    */
  val clusterBuilder: ClusterBuilder = new ClusterBuilder {
    override def buildCluster(builder: Cluster.Builder): Cluster =
      builder
        .addContactPoint(host)
        .withPort(port)
        .withoutJMXReporting()
        .withCodecRegistry(
          new CodecRegistry().register(InstantCodec.instance)
        )
        .build()

  }

  def addSink[E <: ADT: TypeInformation](stream: DataStream[E]): Unit = {
    stream
      .addSink(new CassandraScalaProductSink[E](query, clusterBuilder))
      .uid(label)
      .name(label)
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
  }

  override def _addRowSink(
      stream: DataStream[Row],
      rowType: RowType): Unit = {
    stream.addSink(
      new CassandraRowSink(rowType.getFieldCount, query, clusterBuilder)
    )
    ()
  }
}
