package io.epiphanous.flinkrunner.model.sink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkConnectorName.Jdbc
import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkConnectorName,
  FlinkEvent,
  JdbcStatementParam
}
import org.apache.flink.connector.jdbc.JdbcStatementBuilder

import java.util.Properties

case class JdbcSinkConfig(
    config: FlinkConfig,
    connector: FlinkConnectorName = Jdbc,
    name: String,
    url: String,
    query: String,
    params: Seq[JdbcStatementParam],
    properties: Properties)
    extends SinkConfig
    with LazyLogging {
  def getStatementBuilder[E <: FlinkEvent]: JdbcStatementBuilder[E] = {
    case (statement, element) =>
      val data = element.getClass.getDeclaredFields
        .map(_.getName)
        .zip(element.productIterator.toIndexedSeq)
        .toMap
        .filterKeys(f => params.exists(_.name.equalsIgnoreCase(f)))
      params.zipWithIndex.foreach { case (p, i) =>
        val x = i + 1
        data.get(p.name) match {
          case Some(v) => statement.setObject(x, v, p.jdbcType)
          case None    =>
            throw new RuntimeException(
              s"value for field ${p.name} is not in $element"
            )
        }
      }
  }
}
