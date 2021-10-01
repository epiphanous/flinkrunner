package io.epiphanous.flinkrunner.operator

import java.sql.PreparedStatement

abstract class AddToJdbcBatchFunction[E] {

  def addToJdbcStatement(row: E, ps: PreparedStatement): Unit

  def addToBatch(row: E, ps: PreparedStatement): Unit = {
    addToJdbcStatement(row, ps)
    ps.addBatch()
  }

}
