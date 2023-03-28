package io.epiphanous.flinkrunner

import io.epiphanous.flinkrunner.flink.StreamJob
import io.epiphanous.flinkrunner.model._
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.data.RowData

import scala.reflect.runtime.{universe => ru}

trait FlinkRunnerSpec {

  def getRunner[
      IN <: ADT: TypeInformation,
      OUT <: ADT: TypeInformation,
      JF <: StreamJob[OUT, ADT],
      ADT <: FlinkEvent: TypeInformation](
      configStr: String,
      jobFactory: JobFactory[JF, IN, OUT, ADT],
      checkResultsOpt: Option[CheckResults[ADT]] = None,
      args: Array[String] = Array("testJob")): FlinkRunner[ADT] = {
    val config = new FlinkConfig(args, Some(configStr))
    new FlinkRunner[ADT](config, checkResultsOpt) {
      override def invoke(jobName: String): Unit =
        jobFactory.getJob(this).run()
    }
  }

  def getStreamJobRunner[
      IN <: ADT: TypeInformation,
      OUT <: ADT: TypeInformation,
      ADT <: FlinkEvent: TypeInformation](
      configStr: String,
      transformer: MapFunction[IN, OUT],
      input: Seq[IN] = Seq.empty,
      checkResultsOpt: Option[CheckResults[ADT]] = None,
      args: Array[String] = Array("testJob")): FlinkRunner[ADT] = {
    getRunner(
      configStr,
      new StreamJobFactory[IN, OUT, ADT](transformer, input),
      checkResultsOpt,
      args
    )
  }

  def getIdentityStreamJobRunner[
      OUT <: ADT: TypeInformation,
      ADT <: FlinkEvent: TypeInformation](
      configStr: String,
      input: Seq[OUT] = Seq.empty,
      checkResultsOpt: Option[CheckResults[ADT]] = None,
      args: Array[String] = Array("testJob")): FlinkRunner[ADT] =
    getRunner(
      configStr,
      new IdentityStreamJobFactory(input),
      checkResultsOpt,
      args
    )

  def getAvroStreamJobRunner[
      IN <: ADT with EmbeddedAvroRecord[INA]: TypeInformation,
      INA <: GenericRecord: TypeInformation,
      OUT <: ADT with EmbeddedAvroRecord[OUTA]: TypeInformation,
      OUTA <: GenericRecord: TypeInformation,
      ADT <: FlinkEvent: TypeInformation](
      configStr: String,
      transformer: MapFunction[IN, OUT],
      input: Seq[IN] = Seq.empty,
      checkResultsOpt: Option[CheckResults[ADT]] = None,
      args: Array[String] = Array("testJob"))(implicit
      fromKV: EmbeddedAvroRecordInfo[INA] => IN): FlinkRunner[ADT] =
    getRunner(
      configStr,
      new AvroStreamJobFactory[IN, INA, OUT, OUTA, ADT](
        transformer,
        input
      ),
      checkResultsOpt,
      args
    )

  def getIdentityAvroStreamJobRunner[
      OUT <: ADT with EmbeddedAvroRecord[OUTA]: TypeInformation,
      OUTA <: GenericRecord: TypeInformation,
      ADT <: FlinkEvent: TypeInformation](
      configStr: String,
      input: Seq[OUT] = Seq.empty,
      checkResultsOpt: Option[CheckResults[ADT]] = None,
      args: Array[String] = Array("testJob"))(implicit
      fromKV: EmbeddedAvroRecordInfo[OUTA] => OUT): FlinkRunner[ADT] =
    getRunner(
      configStr,
      new IdentityAvroStreamJobFactory[OUT, OUTA, ADT](input),
      checkResultsOpt,
      args
    )

  def getTableStreamJobRunner[
      IN <: ADT with EmbeddedRowType: TypeInformation: ru.TypeTag,
      OUT <: ADT with EmbeddedRowType: TypeInformation: ru.TypeTag,
      ADT <: FlinkEvent: TypeInformation](
      configStr: String,
      transformer: MapFunction[IN, OUT],
      input: Seq[IN] = Seq.empty,
      checkResultsOpt: Option[CheckResults[ADT]] = None,
      args: Array[String] = Array("testJob"))(implicit
      fromRowData: RowData => IN): FlinkRunner[ADT] =
    getRunner(
      configStr,
      new TableStreamJobFactory[IN, OUT, ADT](transformer, input),
      checkResultsOpt,
      args
    )

  def getIdentityTableStreamJobRunner[
      OUT <: ADT with EmbeddedRowType: TypeInformation: ru.TypeTag,
      ADT <: FlinkEvent: TypeInformation](
      configStr: String,
      input: Seq[OUT] = Seq.empty,
      checkResultsOpt: Option[CheckResults[ADT]] = None,
      args: Array[String] = Array("testJob"))(implicit
      fromRowData: RowData => OUT): FlinkRunner[ADT] =
    getRunner(
      configStr,
      new IdentityTableStreamJobFactory[OUT, ADT](input),
      checkResultsOpt,
      args
    )

//  def getAvroTableStreamJobRunner[
//      IN <: ADT with EmbeddedAvroRecord[INA]: TypeInformation,
//      INA <: GenericRecord: TypeInformation,
//      OUT <: ADT with EmbeddedAvroRecord[
//        OUTA
//      ] with EmbeddedRowType: TypeInformation,
//      OUTA <: GenericRecord: TypeInformation,
//      ADT <: FlinkEvent: TypeInformation](
//      configStr: String,
//      transformer: MapFunction[IN, OUT],
//      input: Seq[IN] = Seq.empty,
//      checkResultsOpt: Option[CheckResults[ADT]] = None,
//      args: Array[String] = Array("testJob"))(implicit
//      fromKV: EmbeddedAvroRecordInfo[INA] => IN): FlinkRunner[ADT] =
//    getRunner(
//      configStr,
//      new AvroTableStreamJobFactory[IN, INA, OUT, OUTA, ADT](
//        transformer,
//        input
//      ),
//      checkResultsOpt,
//      args
//    )
//
//  def getIdentityAvroTableStreamJobRunner[
//      OUT <: ADT with EmbeddedAvroRecord[
//        OUTA
//      ] with EmbeddedRowType: TypeInformation,
//      OUTA <: GenericRecord: TypeInformation,
//      ADT <: FlinkEvent: TypeInformation](
//      configStr: String,
//      input: Seq[OUT] = Seq.empty,
//      checkResultsOpt: Option[CheckResults[ADT]] = None,
//      args: Array[String] = Array("testJob"))(implicit
//      fromKV: EmbeddedAvroRecordInfo[OUTA] => OUT): FlinkRunner[ADT] =
//    getRunner(
//      configStr,
//      new IdentityAvroTableStreamJobFactory[OUT, OUTA, ADT](input),
//      checkResultsOpt,
//      args
//    )
}
