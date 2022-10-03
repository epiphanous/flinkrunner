package io.epiphanous.flinkrunner.operator

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkEvent
import io.epiphanous.flinkrunner.model.sink.JdbcSinkConfig
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor
import org.apache.flink.connector.jdbc.internal.{
  GenericJdbcSinkFunction,
  JdbcOutputFormat
}

class CreateTableJdbcSinkFunction[E <: ADT, ADT <: FlinkEvent](
    sinkConfig: JdbcSinkConfig[ADT],
    jdbcOutputFormat: JdbcOutputFormat[
      E,
      E,
      JdbcBatchStatementExecutor[E]
    ])
    extends GenericJdbcSinkFunction[E](jdbcOutputFormat)
    with LazyLogging {

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // only execute the create statement in one of our parallel tasks
    val rtc = getRuntimeContext
    logger.info(
      s"jdbc sink opening: ${rtc.getTaskNameWithSubtasks} (subtask=${rtc.getIndexOfThisSubtask} attempt=${rtc.getAttemptNumber})"
    )
    if (rtc.getIndexOfThisSubtask == 0 && rtc.getAttemptNumber == 0) {
      logger.info(
        s"${rtc.getTaskNameWithSubtasks} invoking maybeCreateTable(${sinkConfig.product.entryName}/${sinkConfig.table}) for jdbc sink ${sinkConfig.name}"
      )
      sinkConfig.maybeCreateTable()
    }
  }
}
