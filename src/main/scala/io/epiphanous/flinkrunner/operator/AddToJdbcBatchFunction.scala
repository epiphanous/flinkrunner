package io.epiphanous.flinkrunner.operator

import io.epiphanous.flinkrunner.model.FlinkEvent

import java.sql.PreparedStatement

abstract class AddToJdbcBatchFunction[E <: FlinkEvent] {

  def addToJdbcStatement(row: E, ps: PreparedStatement): Unit

  def addToBatch(row: E, ps: PreparedStatement): Unit = {
    addToJdbcStatement(row, ps)
    ps.addBatch()
  }

}
