package io.epiphanous.flinkrunner.model

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.util.SqlBuilder

import scala.util.{Success, Try}

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
    unique: Boolean = false)
    extends LazyLogging {

  def matches(existing: IndexInfo): Boolean = {
    val nameMatches            = name.equalsIgnoreCase(existing.name)
    val columnsMatch           = {
      columns.size == existing.columns.size && existing.columns.forall(
        ic => {
          Try(columns(ic.position - 1)) match {
            case Success((col, ord)) =>
              ic.name.equalsIgnoreCase(col) && ic.direction.forall(
                _ == ord
              )
            case _                   => false
          }
        }
      )
    }
    val uniqueMatches          = unique == existing.unique
    val allMatch               = nameMatches && columnsMatch && uniqueMatches
    def eq(b: Boolean): String = if (b) "==" else "!="
    logger.debug(
      Seq(
        s"name:$name${eq(nameMatches)}${existing.name}",
        s"columns:($columns)${eq(columnsMatch)}${existing.columns}",
        s"unique:${if (unique) "yes" else "no"}${eq(uniqueMatches)}${if (existing.unique) "yes"
          else "no"}"
      ).mkString(
        s"index matches[${if (allMatch) "YES" else "NO"}](",
        ", ",
        ")"
      )
    )
    allMatch
  }

  def definition(
      database: String,
      schema: String,
      table: String,
      product: SupportedDatabase): String = {
    val sqlBuilder = SqlBuilder(product)
    sqlBuilder.append("CREATE ")
    if (unique) sqlBuilder.append("UNIQUE ")
    sqlBuilder
      .append("INDEX ")
      .identifier(name)
      .append(" ON ")
      .identifier(database, schema, table)
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
