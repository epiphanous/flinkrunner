package io.epiphanous.flinkrunner.model.sink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.SupportedDatabase.{
  Postgresql,
  Snowflake
}
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.model.sink.JdbcSinkConfig.{
  DEFAULT_CONNECTION_TIMEOUT,
  DEFAULT_TIMESCALE_CHUNK_TIME_INTERVAL,
  DEFAULT_TIMESCALE_NUMBER_PARTITIONS
}
import io.epiphanous.flinkrunner.util.SqlBuilder
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor
import org.apache.flink.connector.jdbc.internal.{
  GenericJdbcSinkFunction,
  JdbcOutputFormat
}
import org.apache.flink.connector.jdbc.{
  JdbcConnectionOptions,
  JdbcExecutionOptions,
  JdbcStatementBuilder
}
import org.apache.flink.streaming.api.scala.DataStream

import java.sql.{Connection, DriverManager}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/** A JDBC sink configuration. This configuration currently supports four
  * database types:
  *
  *   - mysql
  *   - postgres
  *   - sql server
  *   - snowflake
  *   - timescale
  *
  * Sink specific configuration values include:
  *
  *   - `connection`: object defining the jdbc connection to the database,
  *     with properties:
  *     - `database`: name of the database (required)
  *     - `schema`: name of the schema (required)
  *     - `url`: jdbc url (required).
  *     - `username`: database user (optional, but if not provided here
  *       must be provided in the `url`)
  *     - `password`: database password (optional, but if not provided here
  *       must be provided in the `url`)
  *     - `timeout`: duration to wait for a connection (optional, defaults
  *       to `5s`)
  *   - `execution`: optional object defining execution parameters:
  *     - `batch`: object defining how batches of inserts are sent to the
  *       database, with properties:
  *       - `interval`: batches are sent at least this often (defaults to
  *         0, which deactivates interval checking)
  *       - `size`: batches of no more than this size are sent at once
  *         (defaults to 5000)
  *   - `table`: required object defining the structure of the database
  *     table data is inserted into
  *     - `name`: name of the table (required)
  *     - `timescale`: optional object defining timescale specific
  *       parameters
  *       - `time.column`: name of the time partitioning column ( required
  *         )
  *       - `chunk.time.interval`: interval in which chunks are aggregated
  *         ( optional ) default 7 days
  *       - `partitioning.column`: name of second partitioning column (
  *         optional )
  *       - `number.partitions`: > 0 ( required if partitioning.column is
  *         set )
  *     - `recreate.objects.if.same`: optional boolean (defaults to false)
  *       that, if true, will drop and recreate objects (tables or indexes)
  *       that exist in the database even if they are the same as their
  *       configured definition. This is useful if you have changed the
  *       logic of how records are inserted and want to reprocess data.
  *     - `columns`: required array defining the columns in the table, with
  *       properties:
  *       - `name`: name of the column (required)
  *       - `type`: jdbc standard data type of the column (required). Note
  *         you may not be able to use the database specific type name here
  *         (unless it happens to overlap with the jdbc name).
  *       - `precision`: total width of the column (optional, but probably
  *         required for character types at least)
  *       - `scale`: fractional digits of numeric columns (optional, mainly
  *         useful for time or decimal types)
  *       - `nullable`: true if the column can contain nulls (optional,
  *         defaults to true)
  *     - `indexes`: optional array defining the indexes that should exist
  *       for the table, with properties:
  *       - `name`: name of the index (required, the table name will be
  *         automatically prefixed to this name, separated by an
  *         underscore)
  *       - `columns`: a list of column names, each optionally suffixed
  *         with a sort order (ASC or DESC, defaults to ASC).
  *       - `unique`: an optional boolean indicating if the index contains
  *         unique values (defaults to false)
  *
  * @param name
  *   name of the sink
  * @param config
  *   flinkrunner configuration
  * @tparam ADT
  *   flinkrunner algebraic data type
  */
case class JdbcSinkConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig)
    extends SinkConfig[ADT]
    with LazyLogging {

  override val connector: FlinkConnectorName = FlinkConnectorName.Jdbc

  val database: String           = config.getString(pfx("connection.database"))
  val schema: String             =
    config.getStringOpt(pfx("connection.schema")).getOrElse("_ignore_")
  val url: String                = config.getString(pfx("connection.url"))
  val product: SupportedDatabase = SupportedDatabase.fromUrl(url)
  val driverName: String         = SupportedDatabase.driverFor(product)

  val username: Option[String] =
    config.getStringOpt(pfx("connection.username"))
  val password: Option[String] =
    config.getStringOpt(pfx("connection.password"))

  val connTimeout: Int    =
    config
      .getDurationOpt(pfx("connection.timeout"))
      .map(_.toSeconds.toInt)
      .getOrElse(DEFAULT_CONNECTION_TIMEOUT)
  val batchInterval: Long =
    config
      .getDurationOpt(pfx("execution.batch.interval"))
      .map(_.toMillis)
      .getOrElse(
        0L
      ) // JdbcExecutionOptions.DEFAULT_INTERVAL_MILLIS is, for unknown reasons, private
  val batchSize: Int  =
    config
      .getIntOpt(pfx("execution.batch.size"))
      .getOrElse(JdbcExecutionOptions.DEFAULT_SIZE)
  val maxRetries: Int =
    config
      .getIntOpt(pfx("execution.max.retries"))
      .getOrElse(JdbcExecutionOptions.DEFAULT_MAX_RETRY_TIMES)

  val recreateObjectsIfSame: Boolean =
    config
      .getBooleanOpt(pfx("table.recreate.objects.if.same"))
      .getOrElse(false)

  val table: String   = config.getString(pfx("table.name"))
  val pkIndex: String = s"pk_$table"

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
        Try(c.getInt("primary.key")).toOption
      )
    )

  val indexes: Seq[JdbcSinkIndex] = Try(
    config.getObjectList(pfx("table.indexes"))
  ) match {
    case Success(list) =>
      list
        .map(_.toConfig)
        .map(c =>
          JdbcSinkIndex(
            c.getString("name"),
            Try(c.getStringList("columns").asScala)
              .getOrElse(Seq(c.getString("columns")))
              .toList
              .map { s =>
                val colAndOrder = s.split("\\s+", 2)
                val col         = colAndOrder.head
                val order       = IndexColumnOrder.stringToOrder(
                  colAndOrder.tail.headOption.getOrElse("ASC")
                )
                (col, order)
              },
            Try(c.getBoolean("unique")).getOrElse(false)
          )
        )
    case _             => Seq.empty
  }

  val isTimescale: Boolean = config
    .getObjectOption(pfx("table.timescale"))
    .nonEmpty

  val timescaleTimeColumn: Option[String] = config
    .getStringOpt(pfx("table.timescale.time.column"))

  val timescaleChunkTimeInterval: String = config
    .getStringOpt(pfx("table.timescale.chunk.time.interval"))
    .getOrElse(DEFAULT_TIMESCALE_CHUNK_TIME_INTERVAL)

  val timescalePartitioningColumn: Option[String] = config
    .getStringOpt(pfx("table.timescale.partitioning.column"))

  val timescaleNumberPartitions: Int = config
    .getIntOpt(pfx("table.timescale.number.partitions"))
    .getOrElse(DEFAULT_TIMESCALE_NUMBER_PARTITIONS)

  val sqlBuilder: SqlBuilder = SqlBuilder(product)

  val dropTableSql: String = sqlBuilder
    .append("DROP TABLE ")
    .identifier(database, schema, table)
    .getSqlAndClear

  val pkCols: Seq[JdbcSinkColumn] = columns
    .filter(_.primaryKey.nonEmpty)
    .sortBy(_.primaryKey.get)

  val pkColsList: String = {
    pkCols.zipWithIndex.foreach { case (col, i) =>
      sqlBuilder.identifier(col.name)
      if (i < pkCols.length - 1) sqlBuilder.append(", ")
    }
    sqlBuilder.getSqlAndClear
  }

  val nonPkCols: Seq[JdbcSinkColumn] =
    columns.filterNot(c => pkCols.contains(c))

  val createTableSql: String = {
    sqlBuilder
      .append("CREATE TABLE ")
      .identifier(database, schema, table)
      .append(" (\n")
    columns.zipWithIndex.foreach { case (column, i) =>
      sqlBuilder
        .append("  ")
        .identifier(column.name)
        .append(" ")
        .append(column.fullTypeString(product))
      if (i < columns.length - 1) sqlBuilder.append(",\n")
    }
    if (pkCols.nonEmpty) {
      sqlBuilder
        .append(",\n  CONSTRAINT ")
        .identifier(s"pk_$table")
        .append(s" PRIMARY KEY ($pkColsList)")
    }
    sqlBuilder.append("\n)")
    sqlBuilder.getSqlAndClear
  }

  val createIndexesSql: Map[String, String] =
    indexes
      .map(index =>
        index.name -> index.definition(database, schema, table, product)
      )
      .toMap

  def buildColumnList(
      cols: Seq[JdbcSinkColumn] = columns,
      assign: Option[String] = None): Unit = {
    val n = cols.length - 1
    cols.zipWithIndex.foreach { case (col, i) =>
      sqlBuilder.identifier(col.name)
      assign.foreach { eq =>
        sqlBuilder.append(eq).identifier(col.name)
        if (eq.endsWith("(")) sqlBuilder.append(")")
      }
      if (i < n) sqlBuilder.append(", ")
    }
  }

  val queryDml: String = {
    sqlBuilder
      .append("INSERT INTO ")
      .identifier(database, schema, table)
      .append(" (")
    buildColumnList()
    sqlBuilder.append(")\nSELECT ")
    Range(0, columns.length).foreach { i =>
      (columns(i).dataType, product) match {
        case (SqlColumnType.JSON, SupportedDatabase.Snowflake)  =>
          sqlBuilder.append("PARSE_JSON(?)")
        case (SqlColumnType.JSON, SupportedDatabase.Postgresql) =>
          sqlBuilder.append("CAST(? AS JSON)")
        case _                                                  => sqlBuilder.append("?")
      }
      if (i < columns.length - 1) sqlBuilder.append(", ")
    }
    product match {
      case SupportedDatabase.Postgresql =>
        if (!isTimescale) {
          sqlBuilder
            .append("\nON CONFLICT ON CONSTRAINT ")
            .identifier(pkIndex)
            .append(" DO UPDATE SET\n")
          buildColumnList(nonPkCols, Some("=EXCLUDED."))
        }

      case SupportedDatabase.Mysql =>
        sqlBuilder.append("\nON DUPLICATE KEY UPDATE\n")
        buildColumnList(nonPkCols, Some("=VALUES("))

      case SupportedDatabase.Snowflake =>
      // do nothing: upsert not supported in a single prepared statement (use merge in stored proc?)

      case SupportedDatabase.SqlServer =>
      // do nothing: upsert not supported in a single prepared statement (use merge in stored proc?)
    }
    sqlBuilder.getSqlAndClear
  }
  logger.debug(
    s"$product generated insert statement for sink $name:\n====\n$queryDml\n====\n"
  )

  def getConnection: Try[Connection] = Try {
    Class.forName(SupportedDatabase.driverFor(product))
    (username, password) match {
      case (Some(u), Some(p)) => DriverManager.getConnection(url, u, p)
      case _                  => DriverManager.getConnection(url)
    }
  }

  /** Synchronizes the sink's table configuration with the database. Note
    * this happens in the first attempt of the first sink task. All DDL
    * statements are executed in a transaction, so will either all succeed
    * and be committed or, if any fail, will be rolled back.
    */
  def maybeCreateTable(): Unit = {
    val indexMessage =
      if (indexes.nonEmpty)
        s" and its index${if (indexes.size > 1) "es" else ""}"
      else ""
    val logMessage   =
      s"synchronize $product $table$indexMessage for jdbc sink $name"

    logger.info(s"attempting to $logMessage")
    val sep = "\n====\n"
    logger.debug(
      s"$product generated table/index DML statements:${(Seq(dropTableSql, createTableSql) ++ createIndexesSql)
          .mkString(sep, sep, sep)}"
    )
    getConnection match {
      case Failure(error) =>
        throw new RuntimeException(
          s"failed to connect to $product database for jdbc sink $name",
          error
        )
      case Success(conn)  =>
        handleTableObjects(conn) match {
          case Success(_)     =>
            logger.info(s"[completed] $logMessage")
          case Failure(error) =>
            conn.close()
            throw new RuntimeException(
              s"failed to $logMessage: ${error.getMessage}",
              error
            )
        }
    }
  }

  /** This method tries to be smart about (re-)creating this sink's
    * requested table and indexes if they don't exist or have changed
    * definitions (or the config
    * <code>table.recreate.objects.if.same</code> is set to true).
    * @param conn
    *   the jdbc connection object
    * @return
    */
  def handleTableObjects(conn: Connection): Try[Unit] = Try {
    val metadata        = conn.getMetaData
    val existingColumns =
      metadata.getColumns(database, schema, table, null)

    var existingColumnCount: Int   = 0
    var sameColumnCount: Int       = 0
    while (existingColumns.next()) {
      existingColumnCount += 1
      val columnName    = existingColumns.getString("COLUMN_NAME")
      val dataType      = existingColumns.getInt("DATA_TYPE")
      val typeName      = existingColumns.getString("TYPE_NAME")
      val columnSize    = existingColumns.getInt("COLUMN_SIZE")
      val decimalDigits = existingColumns.getInt("DECIMAL_DIGITS")
      val isNullable    =
        existingColumns.getString("IS_NULLABLE").toLowerCase match {
          case "yes" => Some(true)
          case "no"  => Some(false)
          case _     => None
        }
      if (
        columns.exists(
          _.matches(
            columnName,
            dataType,
            typeName,
            columnSize,
            decimalDigits,
            isNullable
          )
        )
      ) sameColumnCount += 1
    }
    val tableExists: Boolean       = existingColumnCount > 0
    val newDefinitionSame: Boolean =
      existingColumnCount == columns.size && sameColumnCount == existingColumnCount

    val createTable: Boolean =
      !tableExists || !newDefinitionSame || (newDefinitionSame && recreateObjectsIfSame)

    val dropTable: Boolean = createTable && tableExists

    conn.setAutoCommit(false)
    val stmt = conn.createStatement()

    val existingIndexes =
      metadata.getIndexInfo(database, schema, table, false, false)

    val existingIndexesInfo = mutable.Map.empty[String, IndexInfo]
    while (existingIndexes.next()) {
      val indexName = existingIndexes.getString("INDEX_NAME")
      if (!indexName.equalsIgnoreCase(pkIndex)) {
        val columnName                          = existingIndexes.getString("COLUMN_NAME")
        val ordinalPosition                     = existingIndexes.getInt("ORDINAL_POSITION")
        val ascOrDesc: Option[IndexColumnOrder] =
          Option(existingIndexes.getString("ASC_OR_DESC")).map {
            case "D" => DESC
            case _   => ASC
          }
        val unique                              = !existingIndexes.getBoolean("NON_UNIQUE")
        val indexColumn                         =
          IndexColumn(columnName, ordinalPosition, ascOrDesc)
        existingIndexesInfo.update(
          indexName.toLowerCase,
          existingIndexesInfo
            .get(indexName.toLowerCase)
            .map(info => info.copy(columns = indexColumn :: info.columns))
            .getOrElse(IndexInfo(indexName, unique, List(indexColumn)))
        )
      }
    }

    val existingIndexesToDrop =
      mutable.Map.empty[String, Boolean]
    existingIndexesInfo.foreach { case (key, existingIndexInfo) =>
      existingIndexesToDrop.put(
        key,
        indexes
          .find(_.name.equalsIgnoreCase(key))
          .map(_.matches(existingIndexInfo)) match {
          case Some(true)  => recreateObjectsIfSame
          case Some(false) => true
          case None        => true
        }
      )
    }

    existingIndexesToDrop.foreach { case (key, drop) =>
      if (drop) {
        existingIndexesInfo.get(key).map { info =>
          logger.info(
            s"dropping existing index ${info.name} of table $table for sink $name"
          )
          stmt.execute(
            sqlBuilder
              .append("DROP INDEX ")
              .identifier(database, schema, info.name)
              .getSqlAndClear
          )
        }
      }
    }

    if (dropTable) {
      logger.info(s"dropping existing $table for sink $name")
      stmt.execute(dropTableSql)
    }

    if (createTable) {
      logger.info(s"creating $table for sink $name")
      stmt.execute(createTableSql)
    }

    if (!dropTable && !createTable) {
      logger.info(
        s"leaving existing table [$table] definition in place for sink $name"
      )
    }

    if (product != Snowflake)
      createIndexesSql.foreach { case (indexName, createIndexSql) =>
        if (existingIndexesToDrop.getOrElse(indexName.toLowerCase, true)) {
          logger.info(
            s"creating index [$indexName] of table [$table] for sink $name"
          )
          stmt.execute(createIndexSql)
        }
      }

    /** If the sink is a PostgresDB with Timescale extension creates
      * hypertable, with a partitioning column.
      */
    if (product == Postgresql && isTimescale) {
      val createHypertableDml: String = {
        if (timescaleTimeColumn.isEmpty) {
          throw new RuntimeException(
            s"timescale.time.column must be present in timescale config block"
          )
        }

        sqlBuilder
          .append(
            s"SELECT create_hypertable('$table', '${timescaleTimeColumn.get}'"
          )
          .append(
            s", chunk_time_interval => INTERVAL '$timescaleChunkTimeInterval'"
          )

        if (timescalePartitioningColumn.isDefined) {
          sqlBuilder
            .append(
              s", partitioning_column => '${timescalePartitioningColumn.get}'"
            )
            .append(s", number_partitions => $timescaleNumberPartitions")
        }

        sqlBuilder.append(");").getSqlAndClear
      }

      if (createTable) {
        logger.info(
          s"creating hypertable for [$table]: \n====\n$createHypertableDml\n====\n"
        )
        stmt.executeQuery(createHypertableDml)
      }
    }

    stmt.close()
    conn.commit()
    conn.close()
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

  def _addSink[E <: ADT: TypeInformation](
      dataStream: DataStream[E],
      statementBuilder: JdbcStatementBuilder[E]): Unit = {
    val jdbcOutputFormat =
      new JdbcOutputFormat[E, E, JdbcBatchStatementExecutor[E]](
        new BasicJdbcConnectionProvider(
          getJdbcConnectionOptions,
          properties
        ),
        getJdbcExecutionOptions,
        new JdbcSinkStatementExecutorFactory[E, ADT](
          queryDml,
          statementBuilder
        ),
        JdbcOutputFormat.RecordExtractor.identity[E]
      )
    maybeCreateTable()
    dataStream
      .addSink(
        new GenericJdbcSinkFunction[E](jdbcOutputFormat)
      )
      .uid(label)
      .name(label)
  }

  override def addSink[E <: ADT: TypeInformation](
      dataStream: DataStream[E]): Unit =
    _addSink(dataStream, new JdbcSinkStatementBuilder[E, ADT](columns))

  override def addAvroSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      dataStream: DataStream[E]): Unit =
    _addSink(
      dataStream,
      new JdbcSinkAvroStatementBuilder[E, A, ADT](columns)
    )
}

object JdbcSinkConfig {
  final val DEFAULT_CONNECTION_TIMEOUT            = 5
  final val DEFAULT_TIMESCALE_CHUNK_TIME_INTERVAL = "7 days"
  final val DEFAULT_TIMESCALE_NUMBER_PARTITIONS   = 4
}
