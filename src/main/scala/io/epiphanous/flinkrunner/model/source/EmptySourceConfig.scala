package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.model._
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{
  DataStream,
  StreamExecutionEnvironment
}
import org.apache.flink.table.data.{GenericRowData, RowData}

case class EmptySourceConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig)
    extends SourceConfig[ADT] {
  override def connector: FlinkConnectorName = FlinkConnectorName.Empty

  def _emptySource[E: TypeInformation](
      env: StreamExecutionEnvironment): DataStream[E] = {
    val x = env.fromCollection(Seq.empty[E])
    x
  }

  override def getSourceStream[E <: ADT: TypeInformation](
      env: StreamExecutionEnvironment): DataStream[E] =
    _emptySource[E](env)

  override def getAvroSourceStream[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      env: StreamExecutionEnvironment)(implicit
      fromKV: EmbeddedAvroRecordInfo[A] => E): DataStream[E] =
    _emptySource[E](env)

  override def getRowSourceStream[
      E <: ADT with EmbeddedRowType: TypeInformation](
      env: StreamExecutionEnvironment)(implicit
      fromRowData: RowData => E): DataStream[E] =
    _emptySource[GenericRowData](env).map(fromRowData)

}
