package io.epiphanous.flinkrunner.model

import com.typesafe.scalalogging.LazyLogging

/** A small product to store the name and type of a field that is being
  * written to the sink. This is used to construct a dynamic jdbc statement
  * for the jdbc sink.
  *
  * @see
  *   SqlColumnType
  *
  * @param name
  *   the name of the parameter (must be the same as the field going to the
  *   sink)
  * @param dataType
  *   the name of the SqlColumnType
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
    dataType: SqlColumnType,
    precision: Option[Int] = None,
    scale: Option[Int] = None,
    nullable: Boolean = true,
    primaryKey: Option[Int] = None)
    extends LazyLogging {

  def matches(
      columnName: String,
      jdbcType: Int,
      typeName: String,
      columnSize: Int,
      decimalDigits: Int,
      isNullable: Option[Boolean]): Boolean = {
    val nameMatches            = columnName.equalsIgnoreCase(name)
    val typeMatches            = dataType.jdbcType == jdbcType
    val precisionMatches       = precision.isEmpty || precision.get == columnSize
    val scaleMatches           = scale.isEmpty || scale.get == decimalDigits
    val nullableMatches        = isNullable.nonEmpty || isNullable.get == nullable
    val allMatch               =
      nameMatches && typeMatches && precisionMatches && scaleMatches && nullableMatches
    def eq(b: Boolean): String = if (b) "==" else "!="
    logger.debug(
      Seq(
        s"name:$name${eq(nameMatches)}$columnName",
        s"type:${dataType.name}${eq(typeMatches)}$typeName",
        s"precision:${precision.map(_.toString).getOrElse("_")}${eq(precisionMatches)}$columnSize",
        s"scale:${scale.map(_.toString).getOrElse("_")}${eq(scaleMatches)}${if (Option(decimalDigits).isEmpty) "_"
          else decimalDigits.toString}",
        s"nullable:${if (nullable) "yes" else "no"}${eq(
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

  def fullTypeString(product: SupportedDatabase): String =
    dataType.typeString(product, precision, scale, nullable)
}

object JdbcSinkColumn {
  def apply(
      name: String,
      dataType: String,
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
    SqlColumnType.values
      .get(dataType.toUpperCase)
      .map(sqlColumnType =>
        JdbcSinkColumn(
          name,
          sqlColumnType,
          precision,
          scale,
          primaryKey.isEmpty && nullable,
          primaryKey
        )
      )
      .getOrElse(
        throw new RuntimeException(
          s"Unknown sql column type $dataType"
        )
      )
  }
}
