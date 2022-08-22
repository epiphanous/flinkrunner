package io.epiphanous.flinkrunner.model

import com.typesafe.scalalogging.LazyLogging
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.sql.`type`.{SqlTypeFactoryImpl, SqlTypeName}
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect

/** A small product to store the name and type of a field that is being
  * written to the sink. This is used to construct a dynamic jdbc statement
  * for the jdbc sink. In the jdbc sink config, you should specify the
  * "jdbc.type" config using the calcite name from their
  * <tt>org.apache.calcite.sql.`type`.SqlTypeName</tt> library.
  *
  * @param name
  *   the name of the parameter (must be the same as the field going to the
  *   sink)
  * @param calciteType
  *   the name of the calcite SqlTypeName
  * @param jdbcType
  *   the jdbc type (as an integer) from java.sql.Types (automatically set
  *   based on the calcite sql type name)
  * @param precision
  *   optional column length for relevant types
  * @param scale
  *   optional column decimal places for relevant types
  * @param nullable
  *   true if this column can be null (defaults to true)
  * @param primaryKey
  *   optional ordinal position of this column in the table's primary key
  */
case class JdbcSinkColumn(
    name: String,
    calciteType: SqlTypeName,
    jdbcType: Int,
    precision: Option[Int] = None,
    scale: Option[Int] = None,
    nullable: Boolean = true,
    primaryKey: Option[Int] = None)
    extends LazyLogging {

  def matches(
      columnName: String,
      dataType: Int,
      typeName: String,
      columnSize: Int,
      decimalDigits: Int,
      isNullable: Option[Boolean]): Boolean = {
    val nameMatches            = columnName.equalsIgnoreCase(name)
    val typeMatches            = dataType == jdbcType
    val precisionMatches       = precision.isEmpty || precision.get == columnSize
    val scaleMatches           = scale.isEmpty || scale.get == decimalDigits
    val nullableMatches        = isNullable.nonEmpty || isNullable.get == nullable
    val allMatch               =
      nameMatches && typeMatches && precisionMatches && scaleMatches && nullableMatches
    def eq(b: Boolean): String = if (b) "==" else "!="
    logger.debug(
      Seq(
        s"name:$name${eq(nameMatches)}$columnName",
        s"type:$calciteType${eq(typeMatches)}$typeName",
        s"precision:${precision.map(_.toString).getOrElse("_")}${eq(precisionMatches)}$columnSize",
        s"scale:${scale.map(_.toString).getOrElse("_")}${eq(scaleMatches)}${if (Option(decimalDigits).isEmpty) "_"
          else decimalDigits.toString}",
        s"nullable:${if (nullableMatches) "yes" else "no"}${eq(
            nullableMatches
          )}${isNullable.map(n => if (n) "yes" else "no").getOrElse("_")}"
      ).mkString(
        s"column matches[${if (allMatch) "YES" else "NO"}](",
        ", ",
        ")"
      )
    )
    allMatch
  }

  def fullTypeString(dialect: SqlDialect): String = {
    val typeFactory    = new SqlTypeFactoryImpl(dialect.getTypeSystem)
    val underlyingType = (precision, scale) match {
      case (None, None)       => typeFactory.createSqlType(calciteType)
      case (Some(p), None)    => typeFactory.createSqlType(calciteType, p)
      case (Some(p), Some(s)) =>
        typeFactory.createSqlType(calciteType, p, s)
      case (None, Some(s))    => // shouldn't happen
        throw new RuntimeException(
          s"precision is not configured for column $name with scale $s"
        )
    }
    JdbcSinkColumn.fixup(
      calciteType,
      dialect,
      typeFactory
        .createTypeWithNullability(
          underlyingType,
          nullable
        )
        .getFullTypeString
    )
  }
}

object JdbcSinkColumn {
  def apply(
      name: String,
      calciteType: String,
      precision: Option[Int],
      scale: Option[Int],
      nullable: Boolean,
      primaryKey: Option[Int]): JdbcSinkColumn = {
    if (precision.isEmpty && scale.nonEmpty)
      throw new RuntimeException(
        s"Column $name has a scale of ${scale.get} but no precision configured"
      )
    if (name.length > 59)
      throw new RuntimeException(
        s"Column $name is too long for target database"
      )
    Option(SqlTypeName.valueOf(calciteType.toUpperCase))
      .map(j =>
        JdbcSinkColumn(
          name,
          j,
          j.getJdbcOrdinal,
          precision,
          scale,
          primaryKey.isEmpty && nullable,
          primaryKey
        )
      )
      .getOrElse(
        throw new RuntimeException(
          s"Unknown calcite jdbc sql type $calciteType"
        )
      )
  }

  def fixup(
      calciteType: SqlTypeName,
      dialect: SqlDialect,
      typeString: String): String = {
    (calciteType, dialect) match {
      case (SqlTypeName.DOUBLE, _: PostgresqlSqlDialect) =>
        typeString.replace("DOUBLE", "DOUBLE PRECISION")
      case _                                             => typeString
    }
  }
}
