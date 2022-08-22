package io.epiphanous.flinkrunner.model
import enumeratum.EnumEntry.{Hyphencase, Lowercase, Snakecase, Uppercase}
import enumeratum._
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.sql.dialect.{
  MssqlSqlDialect,
  MysqlSqlDialect,
  PostgresqlSqlDialect,
  SnowflakeSqlDialect
}
import org.apache.calcite.sql.util.SqlBuilder

import scala.collection.immutable

sealed trait SupportedDatabase
    extends EnumEntry
    with Lowercase
    with Uppercase
    with Snakecase
    with Hyphencase

object SupportedDatabase extends Enum[SupportedDatabase] {
  override def values: immutable.IndexedSeq[SupportedDatabase] = findValues

  case object Postgresql extends SupportedDatabase
  case object Mysql      extends SupportedDatabase
  case object Snowflake  extends SupportedDatabase
  case object SqlServer  extends SupportedDatabase

  final val MYSQL_DRIVER      = "com.mysql.cj.jdbc.Driver"
  final val POSTGRESQL_DRIVER = "org.postgresql.Driver"
  final val SNOWFLAKE_DRIVER  = "net.snowflake.client.jdbc.SnowflakeDriver"
  final val SQL_SERVER_DRIVER =
    "com.microsoft.sqlserver.jdbc.SQLServerDriver"

  def fromDriver(driverName: String): SupportedDatabase =
    driverName match {
      case MYSQL_DRIVER      => Mysql
      case POSTGRESQL_DRIVER => Postgresql
      case SNOWFLAKE_DRIVER  => Snowflake
      case SQL_SERVER_DRIVER => SqlServer
      case _                 =>
        throw new RuntimeException(s"Unsupported JDBC driver $driverName")
    }

  def driverFor(db: SupportedDatabase) = {
    db match {
      case Mysql      => MYSQL_DRIVER
      case Postgresql => POSTGRESQL_DRIVER
      case Snowflake  => SNOWFLAKE_DRIVER
      case SqlServer  => SQL_SERVER_DRIVER
    }
  }

  def fromUrl(url: String): SupportedDatabase = {
    """jdbc:([^:]+):""".r.findAllIn(url).group(1) match {
      case "mysql"      => Mysql
      case "postgresql" => Postgresql
      case "snowflake"  => Snowflake
      case "sqlserver"  => SqlServer
      case _            =>
        throw new RuntimeException(
          s"invalid jdbc url or unsupported database: $url"
        )
    }
  }

  def dialect(db: SupportedDatabase): SqlDialect = db match {
    case Postgresql => PostgresqlSqlDialect.DEFAULT
    case Mysql      => MysqlSqlDialect.DEFAULT
    case Snowflake  => SnowflakeSqlDialect.DEFAULT
    case SqlServer  => MssqlSqlDialect.DEFAULT
  }

  def qualifiedName(
      db: SupportedDatabase,
      database: String,
      schema: String,
      name: String): String = {
    val sqlBuilder = new SqlBuilder(dialect(db))
    (db match {
      case Postgresql =>
        if (schema.equalsIgnoreCase("public")) sqlBuilder.identifier(name)
        else sqlBuilder.identifier(schema, name)
      case Mysql      => sqlBuilder.identifier(database, name)
      case Snowflake  => sqlBuilder.identifier(database, schema, name)
      case SqlServer  => sqlBuilder.identifier(database, schema, name)
    }).getSqlAndClear
  }
}
