package io.epiphanous.flinkrunner.model

import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.sql.util.SqlBuilder

/** A simple product to support creating an index on the target table for a
  * jdbc sink.
  *
  * @param name
  *   name of the index
  * @param columns
  *   column list to index
  * @param unique
  *   if true, the index should be marked as unique
  */
case class JdbcSinkIndex(
    name: String,
    columns: List[(String, IndexColumnOrder)],
    unique: Boolean = false) {

  def matches(existing: IndexInfo): Boolean =
    false

  def definition(
      qualifiedTableName: String,
      sqlDialect: SqlDialect): String = {
    val sqlBuilder = new SqlBuilder(sqlDialect)
    sqlBuilder.append("CREATE ")
    if (unique) sqlBuilder.append("UNIQUE ")
    sqlBuilder
      .append("INDEX ")
      .identifier(name)
      .append(" ON ")
      .append(qualifiedTableName)
      .append(" (")
    columns.zipWithIndex.foreach { case (col, i) =>
      val (colName, colOrder) = col
      sqlBuilder.identifier(colName).append(" ").append(colOrder.toString)
      if (i < columns.length - 1) sqlBuilder.append(", ")
    }
    sqlBuilder.append(")")
    sqlBuilder.getSqlAndClear
  }

}
