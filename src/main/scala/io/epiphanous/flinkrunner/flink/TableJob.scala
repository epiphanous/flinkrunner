package io.epiphanous.flinkrunner.flink
import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkEvent}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.StatementSet
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import scala.reflect.ClassTag

abstract class TableJob[
    OUT <: ADT: TypeInformation: ClassTag,
    ADT <: FlinkEvent: TypeInformation](runner: FlinkRunner[ADT])
    extends LazyLogging {

  val config: FlinkConfig              = runner.config
  val env: StreamExecutionEnvironment  = runner.env
  val tableEnv: StreamTableEnvironment = runner.tableEnv

  def registerSourcesAndSinks(): Unit =
    config.getSourceNames

  /**
   * do the work to transform the source and return it as a table
   * @return
   *   Table
   */
  def transform(statementSet: StatementSet): Unit

  def run() = {
    registerSourcesAndSinks()
    val statementSet: StatementSet = tableEnv.createStatementSet()
    transform(statementSet)
    statementSet.execute()
  }

}
