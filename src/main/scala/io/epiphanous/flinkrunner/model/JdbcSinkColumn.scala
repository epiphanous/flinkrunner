package io.epiphanous.flinkrunner.model

import org.apache.calcite.sql.`type`.SqlTypeName

/**
 * A small product to store the name and type of a field that is being
 * written to the sink. This is used to construct a dynamic jdbc statement
 * for the jdbc sink. In the jdbc sink config, you should specify the
 * "jdbc.type" config using the calcite name from their
 * <tt>org.apache.calcite.sql.`type`.SqlTypeName</tt> library.
 *
 * @param name
 *   the name of the parameter (must be the same as the field going to the
 *   sink)
 * @param calciteType
 *   the name of the calcite [[SqlTypeName]]
 * @param jdbcType
 *   the jdbc type (as an integer) from [[java.sql.Types]] (automatically
 *   set based on the calcite sql type name)
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
    primaryKey: Option[Int] = None) {
  val len: Option[String] = {
    val l = Seq(
      if (precision.nonEmpty && calciteType.allowsPrec()) precision
      else None,
      if (scale.nonEmpty && calciteType.allowsScale()) scale else None
    ).flatten.mkString("(", ",", ")")
    if (l.nonEmpty) Some(l) else None
  }
  val columnExtra: String = Seq(
    len,
    if (nullable) None else Some(" NOT NULL")
  ).flatten.mkString(" ")
}

object JdbcSinkColumn {
  def apply(
      name: String,
      calciteType: String,
      precision: Option[Int],
      scale: Option[Int],
      nullable: Boolean,
      primaryKey: Option[Int]): JdbcSinkColumn = {
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
          nullable,
          primaryKey
        )
      )
      .getOrElse(
        throw new RuntimeException(
          s"Unknown calcite jdbc sql type $calciteType"
        )
      )
  }
}
