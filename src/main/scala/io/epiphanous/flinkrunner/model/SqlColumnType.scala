package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.model.SqlColumnType.sharedDefaultConfig
import io.epiphanous.flinkrunner.model.SupportedDatabase._

import java.sql.Types

case class SqlColumnType(
    name: String,
    jdbcType: Int,
    defaultConfig: SqlColumnTypeConfig = sharedDefaultConfig,
    productConfig: Map[SupportedDatabase, SqlColumnTypeConfig] =
      Map.empty) {
  def conf(product: SupportedDatabase): SqlColumnTypeConfig =
    productConfig.getOrElse(product, defaultConfig)
  def typeString(
      product: SupportedDatabase,
      precision: Option[Int],
      scale: Option[Int],
      nullable: Boolean): String = {
    val c          = conf(product)
    val typeName   = c.name.getOrElse(name).toUpperCase
    if (!c.isSupported)
      throw new RuntimeException(
        s"unsupported type $typeName for $product"
      )
    val underlying = (precision, scale) match {
      case (None, None) if c.allowsPSNN => typeName
      case (Some(p), None)
          if c.allowsPSYN && p > 0 && p <= c.maxPrecision =>
        s"$typeName($p)"
      case (Some(p), Some(s))
          if c.allowsPSYY && p > 0 && p <= c.maxPrecision && s > 0 && s < p =>
        s"$typeName($p,$s)"
      case _                            =>
        throw new RuntimeException(
          s"invalid type signature for $product type $typeName(prec=${precision
              .getOrElse(".")},scale=${scale.getOrElse(".")}) max prec=${c.maxPrecision}"
        )
    }
    if (nullable) underlying else s"$underlying NOT NULL"
  }
}

object SqlColumnType {
  val sharedDefaultConfig: SqlColumnTypeConfig              = SqlColumnTypeConfig()
  val sharedDefaultConfigWithPrec: SqlColumnTypeConfig      =
    sharedDefaultConfig.copy(signatures =
      SqlColumnTypeConfig.PS_NN | SqlColumnTypeConfig.PS_YN
    )
  val sharedDefaultConfigWithPrecScale: SqlColumnTypeConfig =
    sharedDefaultConfig.copy(signatures =
      SqlColumnTypeConfig.PS_NN | SqlColumnTypeConfig.PS_YN | SqlColumnTypeConfig.PS_YY
    )

  def withName(
      name: String,
      base: SqlColumnTypeConfig = sharedDefaultConfig)
      : SqlColumnTypeConfig = base.copy(name = Some(name))

  def withPrecision(
      precision: Int,
      base: SqlColumnTypeConfig = sharedDefaultConfigWithPrecScale)
      : SqlColumnTypeConfig = base.copy(maxPrecision = precision)

  final val BOOLEAN = SqlColumnType(
    "boolean",
    Types.BOOLEAN,
    productConfig = Map(
      (SqlServer, withName("BIT"))
    )
  )

  final val TINYINT = SqlColumnType(
    "tinyint",
    Types.TINYINT
  )

  final val SMALLINT = SqlColumnType(
    "smallint",
    Types.SMALLINT
  )

  final val BIGINT = SqlColumnType(
    "bigint",
    Types.BIGINT
  )

  final val INTEGER = SqlColumnType(
    "integer",
    Types.INTEGER,
    productConfig = Map(
      Mysql     -> withName("INT"),
      SqlServer -> withName("INT")
    )
  )

  final val NUMERIC = SqlColumnType(
    "numeric",
    Types.NUMERIC,
    withPrecision(38),
    Map(
      Postgresql -> withPrecision(1000),
      Mysql      -> withPrecision(65)
    )
  )

  final val DECIMAL = SqlColumnType(
    "decimal",
    Types.DECIMAL,
    withPrecision(38),
    Map(
      Postgresql -> withPrecision(1000),
      Mysql      -> withPrecision(65)
    )
  )

  final val FLOAT = SqlColumnType(
    "float",
    Types.FLOAT,
    productConfig = Map(
      Postgresql -> withName("REAL")
    )
  )

  final val REAL = SqlColumnType(
    "real",
    Types.REAL,
    withName("FLOAT"),
    Map(
      Postgresql -> withName("REAL")
    )
  )

  final val DOUBLE = SqlColumnType(
    "double",
    Types.DOUBLE,
    productConfig = Map(
      Postgresql -> withName("DOUBLE PRECISION")
    )
  )

  final val CHAR = SqlColumnType(
    "char",
    Types.CHAR,
    sharedDefaultConfigWithPrec,
    productConfig =
      Map(SqlServer -> withName("NCHAR", sharedDefaultConfigWithPrec))
  )

  final val VARCHAR = SqlColumnType(
    "varchar",
    Types.VARCHAR,
    sharedDefaultConfigWithPrec,
    productConfig = Map(
      SqlServer -> withName("NVARCHAR", sharedDefaultConfigWithPrec)
    )
  )

  final val DATE = SqlColumnType(
    "date",
    Types.DATE
  )

  final val TIME = SqlColumnType(
    "time",
    Types.TIME,
    withPrecision(6)
  )

  final val TIME_WITH_TIMEZONE = SqlColumnType(
    "time_with_timezone",
    Types.TIME_WITH_TIMEZONE,
    withPrecision(6)
  )

  final val TIMESTAMP = SqlColumnType(
    "timestamp",
    Types.TIMESTAMP,
    withPrecision(6)
  )

  final val TIMESTAMP_WITH_TIMEZONE = SqlColumnType(
    "timestamp_with_timezone",
    Types.TIMESTAMP_WITH_TIMEZONE,
    withPrecision(6)
  )

  final val JSON = SqlColumnType(
    "json",
    Types.VARCHAR, // Needs to be Types.OTHER for postgres
//    Types.OTHER,
    productConfig = Map(
      Mysql      -> withName("JSON", sharedDefaultConfig),
      Postgresql -> withName("JSONB", sharedDefaultConfig),
      Snowflake  -> withName("VARIANT", sharedDefaultConfig),
      SqlServer  -> withName("NVARCHAR", sharedDefaultConfig)
    )
  )

  final val values = Map(
    "BOOLEAN"                 -> BOOLEAN,
    "TINYINT"                 -> TINYINT,
    "SMALLINT"                -> SMALLINT,
    "BIGINT"                  -> BIGINT,
    "INTEGER"                 -> INTEGER,
    "NUMERIC"                 -> NUMERIC,
    "DECIMAL"                 -> DECIMAL,
    "FLOAT"                   -> FLOAT,
    "REAL"                    -> REAL,
    "DOUBLE"                  -> DOUBLE,
    "CHAR"                    -> CHAR,
    "VARCHAR"                 -> VARCHAR,
    "DATE"                    -> DATE,
    "TIME"                    -> TIME,
    "TIME_WITH_TIMEZONE"      -> TIME_WITH_TIMEZONE,
    "TIMESTAMP"               -> TIMESTAMP,
    "TIMESTAMP_WITH_TIMEZONE" -> TIMESTAMP_WITH_TIMEZONE,
    "JSON"                    -> JSON
  )

}
