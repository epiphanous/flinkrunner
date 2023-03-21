package io.epiphanous.flinkrunner.model

import com.datastax.driver.core.{Cluster, CodecRegistry}
import com.datastax.driver.extras.codecs.jdk8.InstantCodec
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder

@SerialVersionUID(938953971942797516L)
class CassandraClusterBuilder(host: String, port: Int)
    extends ClusterBuilder {
  // not really a bug --|
  //                    V
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
