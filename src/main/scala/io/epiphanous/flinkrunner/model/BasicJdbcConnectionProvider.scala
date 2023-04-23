package io.epiphanous.flinkrunner.model

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.connector.jdbc.JdbcConnectionOptions
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider

import java.sql.{Connection, DriverManager}
import java.util.Properties
import scala.util.Try

/** A basic replacement for flink's SimpleJdbcConnectionProvider, which
  * does not allow using other properties besides user and password when
  * establishing a jdbc connection. This is useful for some jdbc drivers
  * that require more complex security credentials or other special
  * properties to establish a connection.
  * @param jdbcOptions
  *   a flink JdbcConnectionOptions object
  * @param props
  *   arbitrary properties to provide the driver during connection
  */
@SerialVersionUID(2022083817L)
class BasicJdbcConnectionProvider(
    jdbcOptions: JdbcConnectionOptions,
    props: Properties = new Properties())
    extends JdbcConnectionProvider
    with LazyLogging
    with Serializable {

  val url: String = jdbcOptions.getDbURL

  jdbcOptions.getUsername.ifPresent { user =>
    props.setProperty("user", user)
    ()
  }
  jdbcOptions.getPassword.ifPresent { pwd =>
    props.setProperty("password", pwd)
    ()
  }

  @transient
  var connection: Connection = _

  override def getConnection: Connection = connection

  override def getOrEstablishConnection(): Connection =
    Option(connection).getOrElse {
      Option(jdbcOptions.getDriverName).map(Class.forName)
      connection = DriverManager.getConnection(url, props)
      connection
    }

  override def isConnectionValid: Boolean = Option(connection).exists(c =>
    c.isValid(jdbcOptions.getConnectionCheckTimeoutSeconds)
  )

  override def closeConnection(): Unit =
    Try(Option(connection).map(_.close())).fold(
      err => logger.warn("Failed to close jdbc connection", err),
      _ => ()
    )

  override def reestablishConnection(): Connection = {
    closeConnection()
    getOrEstablishConnection()
  }
}
