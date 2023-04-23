package io.epiphanous.flinkrunner.util.test

import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.flink.{
  AvroStreamJob,
  StreamJob,
  TableStreamJob
}
import io.epiphanous.flinkrunner.model.{
  EmbeddedAvroRecord,
  EmbeddedAvroRecordInfo,
  EmbeddedRowType,
  FlinkEvent
}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.data.RowData

import scala.reflect.runtime.{universe => ru}

abstract class JobFactory[
    JF <: StreamJob[OUT, ADT],
    IN <: ADT: TypeInformation,
    OUT <: ADT: TypeInformation,
    ADT <: FlinkEvent: TypeInformation](
    transformer: MapFunction[IN, OUT],
    input: Seq[IN] = Seq.empty,
    sourceName: Option[String] = None)
    extends Serializable {
  def getJob(runner: FlinkRunner[ADT]): JF
}

@SerialVersionUID(1L)
class StreamJobFactory[
    IN <: ADT: TypeInformation,
    OUT <: ADT: TypeInformation,
    ADT <: FlinkEvent: TypeInformation](
    transformer: MapFunction[IN, OUT],
    input: Seq[IN] = Seq.empty,
    sourceName: Option[String] = None)
    extends JobFactory[StreamJob[OUT, ADT], IN, OUT, ADT](
      transformer,
      input,
      sourceName
    ) {
  def getJob(runner: FlinkRunner[ADT]): StreamJob[OUT, ADT] =
    new StreamJob[OUT, ADT](runner) {
      override def transform: DataStream[OUT] =
        seqOrSingleSource(input, sourceName).map(transformer)
    }
}

@SerialVersionUID(1L)
class IdentityStreamJobFactory[
    OUT <: ADT: TypeInformation,
    ADT <: FlinkEvent: TypeInformation](
    input: Seq[OUT] = Seq.empty,
    sourceName: Option[String] = None)
    extends StreamJobFactory[OUT, OUT, ADT](
      new IdentityMap[OUT],
      input,
      sourceName
    )

@SerialVersionUID(1L)
class AvroStreamJobFactory[
    IN <: ADT with EmbeddedAvroRecord[INA]: TypeInformation,
    INA <: GenericRecord: TypeInformation,
    OUT <: ADT with EmbeddedAvroRecord[OUTA]: TypeInformation,
    OUTA <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent: TypeInformation
](
    transformer: MapFunction[IN, OUT],
    input: Seq[IN] = Seq.empty,
    sourceName: Option[String] = None)(implicit
    fromKV: EmbeddedAvroRecordInfo[INA] => IN)
    extends JobFactory[AvroStreamJob[OUT, OUTA, ADT], IN, OUT, ADT](
      transformer,
      input,
      sourceName
    ) {
  def getJob(runner: FlinkRunner[ADT]): AvroStreamJob[OUT, OUTA, ADT] =
    new AvroStreamJob[OUT, OUTA, ADT](runner) {
      override def transform: DataStream[OUT] =
        seqOrSingleAvroSource[IN, INA](input, sourceName).map(transformer)
    }
}

@SerialVersionUID(1L)
class IdentityAvroStreamJobFactory[
    OUT <: ADT with EmbeddedAvroRecord[OUTA]: TypeInformation,
    OUTA <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent: TypeInformation](
    input: Seq[OUT] = Seq.empty,
    sourceName: Option[String] = None)(implicit
    fromKV: EmbeddedAvroRecordInfo[OUTA] => OUT)
    extends AvroStreamJobFactory[OUT, OUTA, OUT, OUTA, ADT](
      new IdentityMap[OUT],
      input,
      sourceName
    )

@SerialVersionUID(1L)
class TableStreamJobFactory[
    IN <: ADT with EmbeddedRowType: TypeInformation: ru.TypeTag,
    OUT <: ADT with EmbeddedRowType: TypeInformation: ru.TypeTag,
    ADT <: FlinkEvent: TypeInformation](
    transformer: MapFunction[IN, OUT],
    input: Seq[IN] = Seq.empty,
    sourceName: Option[String] = None)(implicit fromRowData: RowData => IN)
    extends JobFactory[TableStreamJob[OUT, ADT], IN, OUT, ADT](
      transformer,
      input,
      sourceName
    ) {
  def getJob(runner: FlinkRunner[ADT]): TableStreamJob[OUT, ADT] =
    new TableStreamJob[OUT, ADT](runner) {
      override def transform: DataStream[OUT] =
        seqOrSingleRowSource[IN](input, sourceName).map(transformer)
    }
}

@SerialVersionUID(1L)
class IdentityTableStreamJobFactory[
    OUT <: ADT with EmbeddedRowType: TypeInformation: ru.TypeTag,
    ADT <: FlinkEvent: TypeInformation](
    input: Seq[OUT] = Seq.empty,
    sourceName: Option[String] = None)(implicit
    fromRowData: RowData => OUT)
    extends TableStreamJobFactory[OUT, OUT, ADT](
      new IdentityMap[OUT],
      input,
      sourceName
    )

@SerialVersionUID(1L)
class IdentityMap[A] extends MapFunction[A, A] {
  override def map(value: A): A =
    value
}
