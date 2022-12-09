package io.epiphanous.flinkrunner.model

import com.dimafeng.testcontainers.PostgreSQLContainer
import io.epiphanous.flinkrunner.PropSpec
import org.apache.flink.connector.jdbc.JdbcConnectionOptions
import org.apache.flink.connector.jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder
import org.scalatest

import java.sql.Connection
import scala.util.Try

class BasicJdbcConnectionProviderSpec extends PropSpec {

  val pgContainer: PostgreSQLContainer = PostgreSQLContainer()

  def getJdbcConnectionOptions(
      url: String,
      driverNameOpt: Option[String] = None,
      userOpt: Option[String] = None,
      passwordOpt: Option[String] = None,
      timeoutOpt: Option[Int] = None): JdbcConnectionOptions = {
    val b = new JdbcConnectionOptionsBuilder().withUrl(url)
    driverNameOpt.foreach(b.withDriverName)
    userOpt.foreach(b.withUsername)
    passwordOpt.foreach(b.withPassword)
    timeoutOpt.foreach(b.withConnectionCheckTimeoutSeconds)
    b.build()
  }

  def getConnectionProvider = new BasicJdbcConnectionProvider(
    getJdbcConnectionOptions(
      pgContainer.jdbcUrl,
      driverNameOpt = Some(pgContainer.driverClassName),
      userOpt = Some(pgContainer.username),
      passwordOpt = Some(pgContainer.password)
    )
  )

  property("getOrEstablishConnection property") {
    Try {
      pgContainer.start()
      val c = getConnectionProvider.getOrEstablishConnection()
      Option(c).isEmpty shouldBe false
      pgContainer.stop()
    } shouldBe 'success
  }

  def testConnectionProperty(
      conn: BasicJdbcConnectionProvider => Connection)
      : scalatest.Assertion = {
    Try {
      pgContainer.start()
      val provider = getConnectionProvider
      Option(conn(provider)).isEmpty shouldBe true
      val c        = provider.getOrEstablishConnection()
      conn(provider) shouldEqual c
      pgContainer.stop()
    } shouldBe 'success
  }

  property("connection property") {
    testConnectionProperty((provider: BasicJdbcConnectionProvider) =>
      provider.connection
    )
  }

  property("getConnection property") {
    testConnectionProperty((provider: BasicJdbcConnectionProvider) =>
      provider.getConnection
    )
  }

  property("url property") {
    new BasicJdbcConnectionProvider(
      getJdbcConnectionOptions("test-url")
    ).url shouldEqual "test-url"
  }

  property("isConnectionValid property") {
    Try {
      pgContainer.start()
      val provider = getConnectionProvider
      provider.isConnectionValid shouldBe false
      provider.getOrEstablishConnection()
      provider.isConnectionValid shouldBe true
      pgContainer.stop()
    } shouldBe 'success
  }

  property("closeConnection property") {
    Try {
      pgContainer.start()
      val provider = getConnectionProvider
      provider.closeConnection() // doesn't throw if no connection exists
      val c = provider.getOrEstablishConnection()
      provider.closeConnection()
      c.isClosed shouldBe true
    } shouldBe 'success
  }

  property("reestablishConnection property") {
    Try {
      pgContainer.start()
      val provider = getConnectionProvider
      val c        = provider.reestablishConnection()
      val c2       = provider.reestablishConnection()
      c shouldEqual c2
    } shouldBe 'success
  }

}
