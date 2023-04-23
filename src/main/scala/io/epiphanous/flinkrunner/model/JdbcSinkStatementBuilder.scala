package io.epiphanous.flinkrunner.model

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.serde.JsonRowEncoder
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.connector.jdbc.JdbcStatementBuilder

import java.sql.{PreparedStatement, Timestamp}
import java.time.Instant

class JdbcSinkStatementBuilder[E <: ADT, ADT <: FlinkEvent](
    columns: Seq[JdbcSinkColumn])
    extends JdbcStatementBuilder[E]
    with LazyLogging {

  override def accept(statement: PreparedStatement, event: E): Unit = {
    _fillInStatement(
      fieldValuesOf(event),
      statement,
      event
    )
  }

  def fieldValuesOf[T <: Product](product: T): Map[String, Any] = {
    product.getClass.getDeclaredFields
      .map(_.getName)
      .zip(product.productIterator.toIndexedSeq)
      .toMap
  }

  def _matcher(value: Any): Any = {
    lazy val encoder = new JsonRowEncoder[Map[String, Any]]()
    value match {
      case ts: Instant         => Timestamp.from(ts)
      case m: Map[String, Any] =>
        try
          encoder.encode(m).get
        catch {
          case t: Throwable =>
            logger.error(s"Failed to json encode map: $m\n${t.getMessage}")
            null
        }
      case _                   => value
    }
  }

  def _fillInStatement(
      data: Map[String, Any],
      statement: PreparedStatement,
      element: E): Unit = {
    columns.zipWithIndex.map(x => (x._1, x._2 + 1)).foreach {
      case (column, i) =>
        data.get(column.name) match {
          case Some(v) =>
            val value = v match {
              case null | None => null
              case Some(x)     => _matcher(x)
              case x           => _matcher(x)
            }
            statement.setObject(i, value, column.dataType.jdbcType)
          case None    =>
            throw new RuntimeException(
              s"value for field ${column.name} is not in $element"
            )
        }
    }
  }

}
