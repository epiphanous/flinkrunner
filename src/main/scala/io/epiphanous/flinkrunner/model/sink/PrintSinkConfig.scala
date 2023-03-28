package io.epiphanous.flinkrunner.model.sink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.{
  EmbeddedAvroRecord,
  FlinkConfig,
  FlinkConnectorName,
  FlinkEvent
}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.types.Row

case class PrintSinkConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig
) extends SinkConfig[ADT]
    with LazyLogging {

  override def connector: FlinkConnectorName = FlinkConnectorName.Print

  def _print[T](stream: DataStream[T]): Unit = {
    stream.print()
    ()
  }

  override def addSink[E <: ADT: TypeInformation](
      stream: DataStream[E]): Unit = _print(stream)

  override def addAvroSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](stream: DataStream[E]): Unit =
    _print(stream)

  override def _addRowSink(rows: DataStream[Row], rowType: RowType): Unit =
    _print(rows)
}
