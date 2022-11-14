package io.epiphanous.flinkrunner.model.sink

import com.dimafeng.testcontainers.PostgreSQLContainer
import io.epiphanous.flinkrunner.model.{
  BRecord,
  BWrapper,
  SimpleB,
  StreamFormatName
}
import org.apache.flink.api.scala.createTypeInformation

import java.time.Instant
import java.util.Properties

class JdbcSinkJobTest extends SinkSpec {

  val pgContainer: PostgreSQLContainer = PostgreSQLContainer()

  property("jdbc sink works") {
    pgContainer.start()
    testJob[SimpleB](
      s"""
         |  jdbc-test {
         |    connection  = {
         |      database  = "${pgContainer.databaseName}"
         |      schema    = public
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
         |""".stripMargin,
      "resource://SampleB.csv",
      otherJobConfig = "show.plan = true"
    )

    val props = new Properties()
    props.put("user", pgContainer.username)
    props.put("password", pgContainer.password)
    val conn  =
      pgContainer.jdbcDriverInstance.connect(pgContainer.jdbcUrl, props)
    val stmt  = conn.createStatement()
    val rs    = stmt.executeQuery("select * from sample_b")
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
    pgContainer.stop()
  }

  property("jdbc avro sink works") {
    pgContainer.start()
    val pop      = genPop[BWrapper](10)
    pop.foreach(println)
    val database = pgContainer.databaseName
    val url      = pgContainer.jdbcUrl
    val user     = pgContainer.username
    val pw       = pgContainer.password
    getTempFile(StreamFormatName.Avro).map { path =>
      val avroFile = path.toString
      writeFile(avroFile, StreamFormatName.Avro, pop)
      testAvroJob[BWrapper, BRecord](
        s"""
           |  jdbc-test {
           |    connection  = {
           |      database  = "$database"
           |      schema    = public
           |      url       = "$url"
           |      username  = "$user"
           |      password  = "$pw"
           |    }
           |    table {
           |      name = "b_record"
           |      columns = [
           |        {
           |          name = b0
           |          type = VARCHAR
           |          precision = 36
           |          primary.key = 1
           |        }
           |        {
           |          name = b1
           |          type = INTEGER
           |          nullable = true
           |        }
           |        {
           |          name = b2
           |          type = DOUBLE
           |          nullable = true
           |        }
           |        {
           |          name = b3
           |          type = TIMESTAMP
           |          nullable = false
           |        }
           |      ]
           |    }
           |  }
           |""".stripMargin,
        sourceFile = avroFile,
        sourceFormat = "avro"
      )
      val props    = new Properties()
      props.put("user", user)
      props.put("password", pw)
      val conn     =
        pgContainer.jdbcDriverInstance.connect(url, props)
      val stmt     = conn.createStatement()
      val rs       = stmt.executeQuery("select * from b_record")
      while (rs.next()) {
        val row = rs.getRow
        val b0  = rs.getString("b0").trim()
        val b1  = Option(rs.getInt("b1"))
        val b2  = Option(rs.getDouble("b2"))
        val b3  = Instant.ofEpochMilli(rs.getTimestamp("b3").getTime)
        println(row -> BRecord(b0, b1, b2, b3))
      }
      stmt.close()
      conn.close()
    }
    pgContainer.stop()
  }
}
