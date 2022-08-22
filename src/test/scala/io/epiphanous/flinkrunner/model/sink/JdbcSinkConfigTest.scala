package io.epiphanous.flinkrunner.model.sink

import com.dimafeng.testcontainers.lifecycle.and
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import com.dimafeng.testcontainers.{
  MSSQLServerContainer,
  MultipleContainers,
  MySQLContainer,
  PostgreSQLContainer
}
import io.epiphanous.flinkrunner.UnitSpec
import io.epiphanous.flinkrunner.model.{FlinkConfig, MyAvroADT}

class JdbcSinkConfigTest extends UnitSpec {

  val mysqlContainer: MySQLContainer       = MySQLContainer()
  val pgContainer: PostgreSQLContainer     = PostgreSQLContainer()
  val mssqlContainer: MSSQLServerContainer = MSSQLServerContainer()
  mssqlContainer.container.acceptLicense()

  def doTest(
      database: String,
      schema: String,
      jdbcUrl: String,
      username: String,
      password: String) = {
    val config     = new FlinkConfig(
      Array.empty[String],
      Some(s"""
           |sinks {
           |  jdbc-test {
           |    connector = "jdbc"
           |    connection  = {
           |      database  = "$database"
           |      schema    = "$schema"
           |      url       = "$jdbcUrl"
           |      username  = "$username"
           |      password  = "$password"
           |    }
           |    table = {
           |      name = test-table
           |      columns = [
           |        {
           |          name = id
           |          type = CHAR
           |          precision = 36
           |          primary.key = 1
           |        },
           |        {
           |          name = mocky
           |          type = INTEGER
           |        },
           |        {
           |          name = fishy
           |          type = DOUBLE
           |        }
           |      ]
           |      indexes = [
           |        {
           |          name = mock-index
           |          columns = [mocky,fishy]
           |        }
           |      ]
           |    }
           |  }
           |}
           |""".stripMargin)
    )
    val sinkConfig = new JdbcSinkConfig[MyAvroADT]("jdbc-test", config)
    sinkConfig.maybeCreateTable()
  }

  it should "maybeCreateTable in mysql" in {
    mysqlContainer.start()
    doTest(
      mysqlContainer.databaseName,
      "_default_",
      mysqlContainer.jdbcUrl,
      mysqlContainer.username,
      mysqlContainer.password
    )
    mysqlContainer.stop()
  }

  it should "maybeCreateTable in postgres" in {
    pgContainer.start()
    doTest(
      pgContainer.databaseName,
      "public",
      pgContainer.jdbcUrl,
      pgContainer.username,
      pgContainer.password
    )
    pgContainer.stop()
  }

  ignore should "maybeCreateTable in sql server" in {
    mssqlContainer.start()
    doTest(
      mssqlContainer.databaseName,
      "dbo",
      mssqlContainer.jdbcUrl,
      mssqlContainer.username,
      mssqlContainer.password
    )
    mssqlContainer.stop()
  }

}
