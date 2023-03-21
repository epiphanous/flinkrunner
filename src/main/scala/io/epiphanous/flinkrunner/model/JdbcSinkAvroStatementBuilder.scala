package io.epiphanous.flinkrunner.model

import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.sql.PreparedStatement

class JdbcSinkAvroStatementBuilder[
    E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
    A <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent](columns: Seq[JdbcSinkColumn])
    extends JdbcSinkStatementBuilder[E, ADT](columns) {
  override def accept(statement: PreparedStatement, event: E): Unit = {
    _fillInStatement(
      fieldValuesOf(event.$record.asInstanceOf[Product]),
      statement,
      event
    )
  }
}
