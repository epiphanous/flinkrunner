package io.epiphanous.flinkrunner.model.sink

import io.epiphanous.flinkrunner.flink.StreamJob
import io.epiphanous.flinkrunner.model.{MySimpleADT, SimpleB}
import io.epiphanous.flinkrunner.{FlinkRunner, FlinkRunnerSpec}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.DataStream

import java.sql.DriverManager

class JdbcSinkJobTest extends FlinkRunnerSpec {

//  val pgContainer: PostgreSQLContainer = PostgreSQLContainer()
  val pgContainer = new Object() {
    val databaseName = "test"
    val schema       = "public"
    val jdbcUrl      = "jdbc:postgresql://localhost:5432/test"
    val username     = "test"
    val password     = "test"
  }

  // ignore since it's manual
  ignore("write job results to sink") {
//    pgContainer.start()
    val configStr =
      s"""
        |sinks {
        |  jdbc-test {
        |    connection  = {
        |      database  = "${pgContainer.databaseName}"
        |      schema    = "${pgContainer.schema}"
        |      url       = "${pgContainer.jdbcUrl}"
        |      username  = "${pgContainer.username}"
        |      password  = "${pgContainer.password}"
        |    }
        |    table {
        |      name = "sample_b"
        |      columns = [
        |        {
        |          name = id
        |          type = VARCHAR
        |          precision = 36
        |          primary.key = 1
        |        }
        |        {
        |          name = b0
        |          type = VARCHAR
        |          precision = 255
        |          nullable = false
        |        }
        |        {
        |          name = b1
        |          type = DOUBLE
        |          nullable = false
        |        }
        |        {
        |          name = b2
        |          type = INTEGER
        |        }
        |        {
        |          name = ts
        |          type = TIMESTAMP
        |          nullable = false
        |        }
        |      ]
        |    }
        |  }
        |}
        |sources {
        |  test-file {
        |    path = "resource://SampleB.csv"
        |    format = csv
        |  }
        |}
        |jobs {
        |  testJob {
        |    show.plan = true
        |  }
        |}
        |execution.runtime-mode = batch
        |""".stripMargin
//    val checkResults: CheckResults[MySimpleADT] =
//      new CheckResults[MySimpleADT] {
//        override val name        = "check postgresql table"
//        override val writeToSink = false
//        override def getInputEvents[IN <: MySimpleADT: TypeInformation](
//            sourceConfig: SourceConfig[MySimpleADT]): List[IN] =
//          genPop[SimpleB]().asInstanceOf[List[IN]]
//
//        override def checkOutputEvents[
//            OUT <: MySimpleADT: TypeInformation](
//            sinkConfig: SinkConfig[MySimpleADT],
//            out: List[OUT]): Unit = {
//          logger.debug(out.mkString("\n"))
//          sinkConfig match {
//            case sc: JdbcSinkConfig[MySimpleADT] =>
//              sc.getConnection
//                .fold(
//                  t =>
//                    throw new RuntimeException(
//                      "failed to connect to test database",
//                      t
//                    ),
//                  conn => {
//                    val rs = conn
//                      .createStatement()
//                      .executeQuery(s"select * from ${sc.table}")
//                    while (rs.next()) {
//                      val row = rs.getRow
//                      logger.debug(
//                        s"$row - ${Range(1, 6).map(i => rs.getString(i)).mkString("|")}"
//                      )
//                    }
//                  }
//                )
//            case _                               => logger.debug("Oops")
//          }
//        }
//      }

    val factory = (runner: FlinkRunner[MySimpleADT]) =>
      new SimpleIdentityJob[SimpleB](runner)
    testStreamJob(configStr, factory)
    val conn    = DriverManager.getConnection(
      pgContainer.jdbcUrl,
      pgContainer.username,
      pgContainer.password
    )
    val stmt    = conn.createStatement()
    val rs      = stmt.executeQuery("select * from sample_b")
    while (rs.next()) {
      println(
        rs.getRow + "|" + rs.getString("id").trim() + "|" + rs.getString(
          "b0"
        ) + "|" + rs.getDouble("b1") + "|" + rs.getInt("b2") + "|" + rs
          .getTimestamp(
            "ts"
          )
      )
    }
    stmt.close()
    conn.close()
    //    pgContainer.stop()
  }
}
