package io.epiphanous.flinkrunner.util

import io.epiphanous.flinkrunner.model.{
  JdbcSinkColumn,
  SqlColumnType,
  SupportedDatabase
}

class SnowflakeQueryBuilder extends QueryBuilder {
  override def buildSqlQuery(
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
      (columns(i).dataType) match {
        case (SqlColumnType.JSON) =>
          sqlBuilder.append("parse_json(?)")
        case _                    => sqlBuilder.append("?")
      }
      if (i < columns.length - 1) sqlBuilder.append(", ")
    }
    queryEnd(sqlBuilder, isTimescale, nonPkCols, pkIndex)
    sqlBuilder.getSqlAndClear
  }
}
