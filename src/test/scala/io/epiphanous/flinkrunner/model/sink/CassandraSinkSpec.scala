package io.epiphanous.flinkrunner.model.sink

import com.dimafeng.testcontainers.CassandraContainer
import io.epiphanous.flinkrunner.model.SimpleA
import org.apache.flink.api.scala.createTypeInformation

import java.time.Instant
import scala.collection.JavaConverters._

class CassandraSinkSpec extends SinkSpec {
  val cassandra = new CassandraContainer()

  // todo: fix this test
  ignore("cassandra sink works") {
    cassandra.start()
    val c       = cassandra.container
    val session = c.getCluster.newSession()

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

//    for (
//      row <-
//        session.execute("select * from simple_adt.simple_a").asScala
//    ) {
//      println(row.get(0, classOf[String]))
//      println(row.get(1, classOf[String]))
//      println(row.get(2, classOf[Int]))
//      println(row.get(3, classOf[Instant]))
//    }
    session.close()
    cassandra.stop()
  }
}
