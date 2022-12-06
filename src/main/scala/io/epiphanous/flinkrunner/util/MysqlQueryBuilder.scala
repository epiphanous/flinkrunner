package io.epiphanous.flinkrunner.util

import io.epiphanous.flinkrunner.model.JdbcSinkColumn

class MysqlQueryBuilder extends QueryBuilder {
  override def queryEnd(
      sqlBuilder: SqlBuilder,
      isTimescale: Boolean,
      nonPkCols: Seq[JdbcSinkColumn],
      pkIndex: String): Unit = {
    sqlBuilder.append("\nON DUPLICATE KEY UPDATE\n")
    buildColumnList(sqlBuilder, nonPkCols, Some("=VALUES("))
  }
}
