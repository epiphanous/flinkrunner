package io.epiphanous.flinkrunner.operator

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.sink.JdbcSinkConfig
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor
import org.apache.flink.connector.jdbc.internal.{
  GenericJdbcSinkFunction,
  JdbcOutputFormat
}

class CreateTableJdbcSinkFunction[E](
    sinkConfig: JdbcSinkConfig,
    jdbcOutputFormat: JdbcOutputFormat[
      E,
      E,
      JdbcBatchStatementExecutor[E]])
    extends GenericJdbcSinkFunction[E](jdbcOutputFormat)
    with LazyLogging {

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // only execute the create statement in one of our parallel tasks
    val rtc = getRuntimeContext
    if (rtc.getIndexOfThisSubtask == 0 && rtc.getAttemptNumber == 1) {
      logger.info(
        s"${rtc.getTaskNameWithSubtasks} maybe creating requested jdbc sink target table"
      )
      sinkConfig.maybeCreateTable()
    }
  }
}
