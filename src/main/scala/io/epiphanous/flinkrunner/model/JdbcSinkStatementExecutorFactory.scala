package io.epiphanous.flinkrunner.model

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.connector.jdbc.JdbcStatementBuilder
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat.StatementExecutorFactory
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor

import java.util.function.Function

class JdbcSinkStatementExecutorFactory[E <: ADT, ADT <: FlinkEvent](
    queryDml: String,
    statementBuilder: JdbcStatementBuilder[E])
    extends StatementExecutorFactory[JdbcBatchStatementExecutor[E]] {
  override def apply(rtc: RuntimeContext): JdbcBatchStatementExecutor[E] =
    JdbcBatchStatementExecutor.simple(
      queryDml,
      statementBuilder,
      Function.identity[E]
    )
}
