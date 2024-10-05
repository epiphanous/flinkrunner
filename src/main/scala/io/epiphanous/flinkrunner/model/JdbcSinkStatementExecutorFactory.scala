package io.epiphanous.flinkrunner.model

import org.apache.flink.connector.jdbc.JdbcStatementBuilder
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat.StatementExecutorFactory
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor

class JdbcSinkStatementExecutorFactory[E <: ADT, ADT <: FlinkEvent](
    queryDml: String,
    statementBuilder: JdbcStatementBuilder[E])
    extends StatementExecutorFactory[JdbcBatchStatementExecutor[E]] {

  override def get(): JdbcBatchStatementExecutor[E] =
    JdbcBatchStatementExecutor.simple(
      queryDml,
      statementBuilder
    )
}
