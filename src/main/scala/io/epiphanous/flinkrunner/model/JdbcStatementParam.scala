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
 * @param calciteName
 *   the name of the calcite [[SqlTypeName]] enum entry
 * @param jdbcType
 *   the jdbc type (as an integer) from [[java.sql.Types]]
 */
case class JdbcStatementParam(
    name: String,
    calciteName: String,
    jdbcType: Int)

object JdbcStatementParam {
  def apply(name: String, calciteName: String): JdbcStatementParam =
    Option(SqlTypeName.valueOf(calciteName.toUpperCase))
      .map(j => JdbcStatementParam(name, j.getName, j.getJdbcOrdinal))
      .getOrElse(
        throw new RuntimeException(
          s"Unknown calcite jdbc sql type name $calciteName"
        )
      )
}
