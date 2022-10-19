package io.epiphanous.flinkrunner.model.sink

import com.dimafeng.testcontainers.{
  MSSQLServerContainer,
  MySQLContainer,
  PostgreSQLContainer
}
import io.epiphanous.flinkrunner.UnitSpec
import io.epiphanous.flinkrunner.model.{FlinkConfig, MyAvroADT}

class JdbcSinkCreateTableTest extends UnitSpec {

  val mysqlContainer: MySQLContainer            = MySQLContainer()
  val pgContainer: PostgreSQLContainer          = PostgreSQLContainer()
  val timescaleDbContainer: PostgreSQLContainer = TimescaleDbContainer()
  val mssqlContainer: MSSQLServerContainer      = MSSQLServerContainer()
  mssqlContainer.container.acceptLicense()

  def maybeCreateTableTestTimescale(
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
              |      is.timescale = true
              |      time.column = time
              |      chunk.time.interval = "1 hour"
              |      partitioning.column = device_id
              |      number.partitions = 4
              |      columns = [
              |        {
              |          name = id
              |          type = CHAR
              |          precision = 36
              |        },
              |        {
              |          name = time
              |          type = TIMESTAMP
              |          primary.key = 1
              |        },
              |        {
              |          name = device_id
              |          type = VARCHAR
              |          precision = 100
              |          primary.key = 1
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

  def maybeCreateTableTest(
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
           |          name = mockix2
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
    maybeCreateTableTest(
      mysqlContainer.databaseName,
      "_ignore_",
      mysqlContainer.jdbcUrl,
      mysqlContainer.username,
      mysqlContainer.password
    )
    mysqlContainer.stop()
  }

  it should "maybeCreateTable in postgres" in {
    pgContainer.start()
    maybeCreateTableTest(
      pgContainer.databaseName,
      "public",
      pgContainer.jdbcUrl,
      pgContainer.username,
      pgContainer.password
    )
    pgContainer.stop()
  }

  it should "maybeCreateTable in timescale" in {
    timescaleDbContainer.start()
    maybeCreateTableTestTimescale(
      timescaleDbContainer.databaseName,
      "public",
      timescaleDbContainer.jdbcUrl,
      timescaleDbContainer.username,
      timescaleDbContainer.password
    )
    timescaleDbContainer.stop()
  }

  //ignoring this test now since it relies on manually setting up a local postgres container
  ignore should "maybeCreateTable in postgres local" in {
    maybeCreateTableTest(
      "test",
      "public",
      "jdbc:postgresql://localhost:5432/test",
      "test",
      "test"
    )
  }

  /** ignoring this test as the mssqlcontainer won't start for me
    * -- nextdude 2022/08/22
    */
  ignore should "maybeCreateTable in sql server" in {
    mssqlContainer.start()
    maybeCreateTableTest(
      mssqlContainer.databaseName,
      "dbo",
      mssqlContainer.jdbcUrl,
      mssqlContainer.username,
      mssqlContainer.password
    )
    mssqlContainer.stop()
  }
}
