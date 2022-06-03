package io.epiphanous.flinkrunner.model.sink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkConnectorName.Jdbc
import io.epiphanous.flinkrunner.model.sink.JdbcSinkConfig.DEFAULT_CONNECTION_TIMEOUT
import io.epiphanous.flinkrunner.model._
import org.apache.flink.connector.jdbc.{
  JdbcConnectionOptions,
  JdbcExecutionOptions,
  JdbcStatementBuilder
}

import java.sql.DriverManager
import java.time.Duration
import java.util.Properties
import scala.util.{Failure, Success, Try}

/**
 * A JDBC sink configuration
 * @param config
 *   the flink runner configuration in which this sink is defined
 * @param connector
 *   JDBC
 * @param name
 *   name of the sink
 * @param url
 *   required jdbc url
 * @param username
 *   optional jdbc username (if not provided here, should be embedded in
 *   the url)
 * @param password
 *   optional jdbc password associated with the user (if not provided here,
 *   should be embedded in the url)
 * @param driverName
 *   optional jdbc driver name for the target database (if not provided
 *   here, should be embedded in the url)
 * @param connTimeout
 *   optional timeout for jdbc connection attempts
 * @param batchInterval
 *   a jdbc batch will be sent after this many milliseconds has transpired
 *   since the last batch was sent, if this config is greater than zero
 *   (defaults to zero)
 * @param batchSize
 *   a jdbc batch will be sent once this many rows is accumulated in the
 *   buffer
 * @param maxRetries
 *   max number of times an event will be retried before dropping the event
 *   due to a database connection issue
 * @param createIfNotExists
 *   true if you want to auto-create the target table if it does not
 *   already exist in the database schema (defaults to true)
 * @param schema
 *   the name of the database schema containing the target table
 * @param table
 *   the name of the target table within the schema
 * @param columns
 *   an object defining the structure of the target table (see
 *   [[JdbcSinkColumn]] for the structure of these configs)
 * @param properties
 *   a general java properties object containing any other configs to pass
 *   on to the jdbc sink
 */
case class JdbcSinkConfig(
    config: FlinkConfig,
    connector: FlinkConnectorName = Jdbc,
    name: String,
    dbType: SupportedDatabase,
    url: String,
    username: Option[String],
    password: Option[String],
    driverName: Option[String],
    connTimeout: Option[Duration],
    batchInterval: Option[Duration],
    batchSize: Option[Int],
    maxRetries: Option[Int],
    createIfNotExists: Boolean,
    schema: String,
    table: String,
    columns: Seq[JdbcSinkColumn],
    properties: Properties)
    extends SinkConfig
    with LazyLogging {

  val qTable = s"${qid(schema)}.${qid(table)}"

  /**
   * optional sql code to create the sink's table in the target database
   */
  val createTableDdl: Option[String] =
    if (createIfNotExists)
      Some(s"""
         |CREATE TABLE IF NOT EXISTS $qTable (
         |${columns
        .map(c =>
          s"${qid(c.name)} ${c.calciteType.getName}${c.columnExtra}"
        )
        .mkString("  ", ",\n  ", ",\n  ")}
         |${columns
        .filter(_.primaryKey.nonEmpty)
        .sortBy(_.primaryKey.get)
        .map(_.name)
        .mkString(s"  primary key pk_$table(", ", ", ")")}
        |)
        |""".stripMargin)
    else None

  val queryDml: String = s"""
      |INSERT INTO $qTable
      |(${columns.map(c => qid(c.name)).mkString(", ")})
      |VALUES
      |(${columns.map(_ => "?").mkString(", ")})
      |""".stripMargin

  /** quote an identifier for the target database */
  def qid(ident: String): String = dbType match {
    case SupportedDatabase.Mysql => s"`$ident`"
    case _                       => s""""$ident""""
  }

  def maybeCreateTable(): Unit =
    createTableDdl.foreach { sql =>
      Try {
        Class.forName(SupportedDatabase.driverFor(dbType))
        val conn = (username, password) match {
          case (Some(u), Some(p)) => DriverManager.getConnection(url, u, p)
          case _                  => DriverManager.getConnection(url)
        }
        val stmt = conn.createStatement()
        stmt.execute(sql)
        stmt.close()
        conn.close()
      } match {
        case Success(ok)        =>
          logger.info(s"created $dbType table $qTable")
        case Failure(exception) => throw exception
      }
    }

  def getJdbcExecutionOptions: JdbcExecutionOptions = {
    JdbcExecutionOptions
      .builder()
      .withMaxRetries(
        Option(properties.getProperty("max.retries"))
          .map(o => o.toInt)
          .getOrElse(JdbcExecutionOptions.DEFAULT_MAX_RETRY_TIMES)
      )
      .withBatchSize(
        Option(properties.getProperty("batch.size"))
          .map(o => o.toInt)
          .getOrElse(JdbcExecutionOptions.DEFAULT_SIZE)
      )
      .withBatchIntervalMs(
        Option(properties.getProperty("batch.interval.ms"))
          .map(o => o.toLong)
          .getOrElse(0)
      )
      .build()
  }

  def getJdbcConnectionOptions: JdbcConnectionOptions = {
    val jcoBuilder =
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(url)
    Option(properties.getProperty("connection.username"))
      .foreach(jcoBuilder.withUsername)
    Option(properties.getProperty("connection.password"))
      .foreach(jcoBuilder.withPassword)
    Option(properties.getProperty("connection.driver"))
      .foreach(jcoBuilder.withDriverName)
    Option(
      Duration
        .ofMillis(
          properties
            .getProperty(
              "connection.timeout.ms",
              DEFAULT_CONNECTION_TIMEOUT
            )
            .toLong
        )
        .toSeconds
        .toInt
    ).foreach(jcoBuilder.withConnectionCheckTimeoutSeconds)
    jcoBuilder.build()
  }

  /**
   * Creates a statement builder based on the target table columns and the
   * values in an event
   * @tparam E
   *   the event type
   * @return
   *   JdbcStatementBuilder[E]
   */
  def getStatementBuilder[E <: FlinkEvent]: JdbcStatementBuilder[E] = {
    case (statement, element) =>
      val data = element.getClass.getDeclaredFields
        .map(_.getName)
        .zip(element.productIterator.toIndexedSeq)
        .toMap
        .filterKeys(f => columns.exists(_.name.equalsIgnoreCase(f)))
      columns.zipWithIndex.map(x => (x._1, x._2 + 1)).foreach {
        case (column, i) =>
          data.get(column.name) match {
            case Some(v) => statement.setObject(i, v, column.jdbcType)
            case None    =>
              throw new RuntimeException(
                s"value for field ${column.name} is not in $element"
              )
          }
      }
  }
}

object JdbcSinkConfig {
  final val DEFAULT_CONNECTION_TIMEOUT = "5"
  final val QUOTE_CHAR                 = "\""
}
