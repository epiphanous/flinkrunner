package io.epiphanous.flinkrunner.model.sink

import com.datastax.driver.core
import com.dimafeng.testcontainers.CassandraContainer
import io.epiphanous.flinkrunner.model.{CheckResults, MySimpleADT, SimpleA}
import io.epiphanous.flinkrunner.{FlinkRunner, FlinkRunnerSpec}
import org.apache.flink.api.scala.createTypeInformation

import java.time.Instant
import scala.collection.JavaConverters._

class CassandraJobTest extends FlinkRunnerSpec {
  val cassandra = new CassandraContainer()

  ignore("sink works") {
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

    val factory = (runner: FlinkRunner[MySimpleADT]) =>
      new SimpleIdentityJob[SimpleA](runner)

//    val checkResults: CheckResults[MySimpleADT] =
//      new CheckResults[MySimpleADT] {
//        override val name: String = "cassandra-test"
//
//        override def getInputEvents[IN <: MySimpleADT](
//            sourceName: String): List[IN] =
//          genPop[SimpleA](collectLimit).asInstanceOf[List[IN]]
//
////        override val writeToSink: Boolean = true
//
//        override val collectLimit: Int = 10
//
//        override def checkOutputEvents[OUT <: MySimpleADT](
//            out: List[OUT]): Unit =
//          out.foreach(println)
//      }
    testStreamJob(
      s"""
         |execution.runtime-mode = batch
         |jobs {
         |  testJob {
         |    sources {
         |      file-source {
         |        path = "resource://SampleA.csv"
         |        format = csv
         |      }
         |    }
         |    sinks {
         |      cassandra-test {
         |        host = ${c.getHost}
         |        port = ${c.getMappedPort(9042)}
         |        query = "insert into simple_adt.simple_a (id, a0, a1, ts) values (?, ?, ?, ?);"
         |      }
         |    }
         |  }
         |}
         |""".stripMargin,
      factory
    )
    for (
      row <-
        session.execute("select * from simple_adt.simple_a").asScala
    ) {
      println(row.get(0, classOf[String]))
      println(row.get(1, classOf[String]))
      println(row.get(2, classOf[Int]))
      println(row.get(3, classOf[Instant]))
    }
    session.close()
    cassandra.stop()
  }
}
