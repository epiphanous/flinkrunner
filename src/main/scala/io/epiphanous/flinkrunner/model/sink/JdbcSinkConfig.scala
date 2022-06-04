package io.epiphanous.flinkrunner.model.sink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkConnectorName.Jdbc
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.model.sink.JdbcSinkConfig.DEFAULT_CONNECTION_TIMEOUT
import io.epiphanous.flinkrunner.operator.CreateTableJdbcSinkFunction
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor
import org.apache.flink.connector.jdbc.{
  JdbcConnectionOptions,
  JdbcExecutionOptions,
  JdbcStatementBuilder
}
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.DataStream

import java.sql.DriverManager
import java.util.function.{Function => JavaFunction}
import scala.util.{Failure, Success, Try}

/** A JDBC sink configuration
  * @param config
  *   the flink runner configuration in which this sink is defined
  * @param connector
  *   JDBC
  * @param name
  *   name of the sink
  */
case class JdbcSinkConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig,
    connector: FlinkConnectorName = Jdbc)
    extends SinkConfig[ADT]
    with LazyLogging {

  val url: String               = config.getString(pfx("url"))
  val dbType: SupportedDatabase =
    config
      .getStringOpt(pfx("connection.driver"))
      .map(d => SupportedDatabase.fromDriver(d))
      .getOrElse(SupportedDatabase.fromUrl(url))
  val driverName: String        = SupportedDatabase.driverFor(dbType)

  val username: Option[String] = config.getStringOpt(pfx("username"))
  val password: Option[String] = config.getStringOpt(pfx("password"))

  val connTimeout: Int             =
    config
      .getDurationOpt(pfx("connection.timeout"))
      .map(_.toSeconds.toInt)
      .getOrElse(DEFAULT_CONNECTION_TIMEOUT)
  val batchInterval: Long          =
    config
      .getDurationOpt(pfx("execution.batch.interval"))
      .map(_.toSeconds)
      .getOrElse(0L)
  val batchSize: Int               =
    config
      .getIntOpt(pfx("execution.batch.size"))
      .getOrElse(JdbcExecutionOptions.DEFAULT_SIZE)
  val maxRetries: Int              =
    config
      .getIntOpt(pfx("execution.max.retries"))
      .getOrElse(JdbcExecutionOptions.DEFAULT_MAX_RETRY_TIMES)
  val createIfNotExists: Boolean   =
    config.getBooleanOpt(pfx("table.create.if.not.exists")).getOrElse(true)
  val schema: String               =
    config.getStringOpt("table.schema").getOrElse("_default_")
  val table: String                = config.getString("table.name")
  val columns: Seq[JdbcSinkColumn] = config
    .getObjectList(pfx("table.columns"))
    .map(_.toConfig)
    .map(c =>
      JdbcSinkColumn(
        c.getString("name"),
        c.getString("type"),
        Try(c.getInt("precision")).toOption,
        Try(c.getInt("scale")).toOption,
        Try(c.getBoolean("nullable")).toOption.getOrElse(true),
        Try(c.getInt("primaryKey")).toOption
      )
    )

  val qTable = s"${if (schema.equalsIgnoreCase("_default_")) ""
    else qid(schema) + "."}${qid(table)}"

  /** optional sql code to create the sink's table in the target database
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
        case Success(_)         =>
          logger.info(s"created $dbType table $qTable")
        case Failure(exception) => throw exception
      }
    }

  def getJdbcExecutionOptions: JdbcExecutionOptions = {
    JdbcExecutionOptions
      .builder()
      .withMaxRetries(maxRetries)
      .withBatchSize(batchSize)
      .withBatchIntervalMs(batchInterval)
      .build()
  }

  def getJdbcConnectionOptions: JdbcConnectionOptions = {
    val jcoBuilder =
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl(url)
        .withDriverName(driverName)
        .withConnectionCheckTimeoutSeconds(connTimeout)
    username.foreach(jcoBuilder.withUsername)
    password.foreach(jcoBuilder.withPassword)
    jcoBuilder.build()
  }

  /** Creates a statement builder based on the target table columns and the
    * values in an event
    * @tparam E
    *   the event type
    * @return
    *   JdbcStatementBuilder[E]
    */
  def getStatementBuilder[E <: ADT]: JdbcStatementBuilder[E] = {
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

  def getSink[E <: ADT: TypeInformation](
      dataStream: DataStream[E]): DataStreamSink[E] = {
    val jdbcOutputFormat =
      new JdbcOutputFormat[E, E, JdbcBatchStatementExecutor[E]](
        new SimpleJdbcConnectionProvider(
          getJdbcConnectionOptions
        ),
        getJdbcExecutionOptions,
        (_: RuntimeContext) =>
          JdbcBatchStatementExecutor.simple(
            queryDml,
            getStatementBuilder[E],
            JavaFunction.identity[E]
          ),
        JdbcOutputFormat.RecordExtractor.identity[E]
      )
    dataStream
      .addSink(
        new CreateTableJdbcSinkFunction[E, ADT](this, jdbcOutputFormat)
      )
      .uid(label)
      .name(label)
  }
}

object JdbcSinkConfig {
  final val DEFAULT_CONNECTION_TIMEOUT = 5
  final val QUOTE_CHAR                 = "\""
}
