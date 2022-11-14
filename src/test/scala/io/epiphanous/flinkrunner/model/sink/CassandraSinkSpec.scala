package io.epiphanous.flinkrunner.model.sink

import com.datastax.driver.core.{Cluster, CodecRegistry}
import com.datastax.driver.extras.codecs.jdk8.InstantCodec
import com.dimafeng.testcontainers.CassandraContainer
import io.epiphanous.flinkrunner.model.SimpleA
import org.apache.flink.api.scala.createTypeInformation

import java.time.Instant
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class CassandraSinkSpec extends SinkSpec {
  val cassandra = new CassandraContainer()

  property("cassandra sink works") {
    cassandra.start()
    val c       = cassandra.container
    val session = Cluster
      .builder()
      .addContactPoint(c.getHost)
      .withPort(c.getMappedPort(9042))
      .withoutJMXReporting()
      .withCodecRegistry(
        new CodecRegistry().register(InstantCodec.instance)
      )
      .build()
      .newSession()

    session.execute("""
        |create keyspace if not exists simple_adt
        |  with replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        |""".stripMargin)

    session.execute("""
        |create table if not exists simple_adt.simple_a (
        |  id text,
        |  a0 text,
        |  a1 int,
        |  ts timestamp,
        |  primary key(id)
        |);
        |""".stripMargin)

    testJob[SimpleA](
      s"""
         |cassandra-test {
         |  host = ${c.getHost}
         |  port = ${c.getMappedPort(9042)}
         |  query = "insert into simple_adt.simple_a (id, a0, a1, ts) values (?, ?, ?, ?);"
         |}
         |""".stripMargin,
      "resource://SampleA.csv"
    )

    // contents of SampleA.csv sorted
    val expected =
      """
        |b0Q1VjB,B-TFWR-9685,226,2022-08-26T20:37:17.299Z
        |bGlSUta,B-GSZQ-8036,245,2022-08-26T20:37:13.649Z
        |dluBK7m,B-RYOT-2386,200,2022-08-26T20:37:39.150Z
        |edopOkb,B-VSFJ-9246,299,2022-08-26T20:37:23.287Z
        |gx21ge6,B-NGMZ-5351,200,2022-08-26T20:37:33.031Z
        |i4t00SY,B-RLTY-8415,223,2022-08-26T20:37:41.671Z
        |nF1kVdP,B-CEWP-2441,299,2022-08-26T20:37:10.113Z
        |w0x0NBB,B-VHHF-7895,217,2022-08-26T20:36:56.524Z
        |zVqYPPA,B-GCWD-8429,301,2022-08-26T20:37:37.854Z
        |""".stripMargin.trim

    val rows = ArrayBuffer.empty[String]
    for (
      row <-
        session
          .execute("select * from simple_adt.simple_a")
          .asScala
    ) {
      rows +=
        List(
          row.get(0, classOf[String]),
          row.get(1, classOf[String]),
          row.get(2, classOf[Int]).toString,
          row.get(3, classOf[Instant]).toString
        ).mkString(",")
    }

    rows.sorted.mkString("\n") shouldEqual expected

    session.close()
    cassandra.stop()
  }
}
