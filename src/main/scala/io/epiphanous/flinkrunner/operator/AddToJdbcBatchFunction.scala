package io.epiphanous.flinkrunner.operator
import java.sql.PreparedStatement

import io.epiphanous.flinkrunner.model.FlinkEvent

abstract class AddToJdbcBatchFunction[E <: FlinkEvent] {

  def addToJdbcStatement(row: E, ps: PreparedStatement)

  def addToBatch(row: E, ps: PreparedStatement): Unit = {
    addToJdbcStatement(row, ps)
    ps.addBatch()
  }

}
