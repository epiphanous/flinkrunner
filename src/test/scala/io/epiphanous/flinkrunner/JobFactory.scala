package io.epiphanous.flinkrunner

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
    input: Seq[IN] = Seq.empty)
    extends Serializable {
  def getJob(runner: FlinkRunner[ADT]): JF
}

@SerialVersionUID(1L)
class StreamJobFactory[
    IN <: ADT: TypeInformation,
    OUT <: ADT: TypeInformation,
    ADT <: FlinkEvent: TypeInformation](
    transformer: MapFunction[IN, OUT],
    input: Seq[IN] = Seq.empty)
    extends JobFactory[StreamJob[OUT, ADT], IN, OUT, ADT](
      transformer,
      input
    ) {
  def getJob(runner: FlinkRunner[ADT]): StreamJob[OUT, ADT] =
    new StreamJob[OUT, ADT](runner) {
      override def transform: DataStream[OUT] =
        seqOrSingleSource(input).map(transformer)
    }
}

@SerialVersionUID(1L)
class IdentityStreamJobFactory[
    OUT <: ADT: TypeInformation,
    ADT <: FlinkEvent: TypeInformation](input: Seq[OUT] = Seq.empty)
    extends StreamJobFactory[OUT, OUT, ADT](new IdentityMap[OUT], input)

@SerialVersionUID(1L)
class AvroStreamJobFactory[
    IN <: ADT with EmbeddedAvroRecord[INA]: TypeInformation,
    INA <: GenericRecord: TypeInformation,
    OUT <: ADT with EmbeddedAvroRecord[OUTA]: TypeInformation,
    OUTA <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent: TypeInformation
](transformer: MapFunction[IN, OUT], input: Seq[IN] = Seq.empty)(implicit
    fromKV: EmbeddedAvroRecordInfo[INA] => IN)
    extends JobFactory[AvroStreamJob[OUT, OUTA, ADT], IN, OUT, ADT](
      transformer,
      input
    ) {
  def getJob(runner: FlinkRunner[ADT]): AvroStreamJob[OUT, OUTA, ADT] =
    new AvroStreamJob[OUT, OUTA, ADT](runner) {
      override def transform: DataStream[OUT] =
        seqOrSingleAvroSource[IN, INA](input).map(transformer)
    }
}

@SerialVersionUID(1L)
class IdentityAvroStreamJobFactory[
    OUT <: ADT with EmbeddedAvroRecord[OUTA]: TypeInformation,
    OUTA <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent: TypeInformation](input: Seq[OUT] = Seq.empty)(
    implicit fromKV: EmbeddedAvroRecordInfo[OUTA] => OUT)
    extends AvroStreamJobFactory[OUT, OUTA, OUT, OUTA, ADT](
      new IdentityMap[OUT],
      input
    )

@SerialVersionUID(1L)
class TableStreamJobFactory[
    IN <: ADT with EmbeddedRowType: TypeInformation: ru.TypeTag,
    OUT <: ADT with EmbeddedRowType: TypeInformation: ru.TypeTag,
    ADT <: FlinkEvent: TypeInformation](
    transformer: MapFunction[IN, OUT],
    input: Seq[IN] = Seq.empty)(implicit fromRowData: RowData => IN)
    extends JobFactory[TableStreamJob[OUT, ADT], IN, OUT, ADT](
      transformer,
      input
    ) {
  def getJob(runner: FlinkRunner[ADT]): TableStreamJob[OUT, ADT] =
    new TableStreamJob[OUT, ADT](runner) {
      override def transform: DataStream[OUT] =
        seqOrSingleRowSource(input).map(transformer)
    }
}

@SerialVersionUID(1L)
class IdentityTableStreamJobFactory[
    OUT <: ADT with EmbeddedRowType: TypeInformation: ru.TypeTag,
    ADT <: FlinkEvent: TypeInformation](input: Seq[OUT] = Seq.empty)(
    implicit fromRowData: RowData => OUT)
    extends TableStreamJobFactory[OUT, OUT, ADT](
      new IdentityMap[OUT],
      input
    )

//@SerialVersionUID(1L)
//class AvroTableStreamJobFactory[
//    IN <: ADT with EmbeddedAvroRecord[INA]: TypeInformation,
//    INA <: GenericRecord: TypeInformation,
//    OUT <: ADT with EmbeddedAvroRecord[OUTA]: TypeInformation,
//    OUTA <: GenericRecord: TypeInformation,
//    ADT <: FlinkEvent: TypeInformation](
//    transformer: MapFunction[IN, OUT],
//    input: Seq[IN] = Seq.empty)(implicit
//    fromKV: EmbeddedAvroRecordInfo[INA] => IN)
//    extends JobFactory[AvroTableStreamJob[OUT, OUTA, ADT], IN, OUT, ADT](
//      transformer,
//      input
//    ) {
//  override def getJob(
//      runner: FlinkRunner[ADT]): AvroTableStreamJob[OUT, OUTA, ADT] =
//    new AvroTableStreamJob[OUT, OUTA, ADT](runner) {
//      override def transform: DataStream[OUT] =
//        seqOrSingleAvroSource[IN, INA](input).map(transformer)
//    }
//}
//
//@SerialVersionUID(1L)
//class IdentityAvroTableStreamJobFactory[
//    OUT <: ADT with EmbeddedAvroRecord[OUTA]: TypeInformation,
//    OUTA <: GenericRecord: TypeInformation,
//    ADT <: FlinkEvent: TypeInformation](input: Seq[OUT] = Seq.empty)(
//    implicit fromKV: EmbeddedAvroRecordInfo[OUTA] => OUT)
//    extends AvroTableStreamJobFactory[OUT, OUTA, OUT, OUTA, ADT](
//      new IdentityMap[OUT],
//      input
//    )

@SerialVersionUID(1L)
class IdentityMap[A] extends MapFunction[A, A] {
  override def map(value: A): A =
    value
}
