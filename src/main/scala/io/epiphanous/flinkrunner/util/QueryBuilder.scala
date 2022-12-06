package io.epiphanous.flinkrunner.util

import io.epiphanous.flinkrunner.model.{
  JdbcSinkColumn,
  SqlColumnType,
  SupportedDatabase
}

import scala.collection.Seq

trait QueryBuilder {
  def buildColumnList(
      sqlBuilder: SqlBuilder,
      cols: Seq[JdbcSinkColumn],
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

  def buildSqlQuery(
      database: String,
      schema: String,
      table: String,
      columns: Seq[JdbcSinkColumn],
      product: SupportedDatabase,
      isTimescale: Boolean,
      nonPkCols: Seq[JdbcSinkColumn],
      pkIndex: String): Unit = {
    val sqlBuilder: SqlBuilder = SqlBuilder(product)

    sqlBuilder
      .append("INSERT INTO ")
      .identifier(database, schema, table)
      .append(" (")
    buildColumnList(sqlBuilder, columns)
    sqlBuilder.append(")\nSELECT ")

    Range(0, columns.length).foreach { i =>
      sqlBuilder.append("?")
      if (i < columns.length - 1) sqlBuilder.append(", ")
    }
    queryEnd(sqlBuilder, isTimescale, nonPkCols, pkIndex)
    sqlBuilder.getSqlAndClear
  }

  def queryEnd(
      sqlBuilder: SqlBuilder,
      isTimescale: Boolean,
      nonPkCols: Seq[JdbcSinkColumn],
      pkIndex: String): Unit = {
    // Will be overriden as necessary in implementations
  }

}
