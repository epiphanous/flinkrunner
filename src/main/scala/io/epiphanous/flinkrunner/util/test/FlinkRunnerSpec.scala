package io.epiphanous.flinkrunner.util.test

import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.flink.StreamJob
import io.epiphanous.flinkrunner.model._
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.data.RowData

import scala.reflect.runtime.{universe => ru}

trait FlinkRunnerSpec {
  val DEFAULT_CONFIG_STR: String =
    """
      |execution.runtime-mode=batch
      |sources{empty-source{}}
      |sinks{print-sink{}}
      |""".stripMargin

  def getDefaultConfig: FlinkConfig =
    new FlinkConfig(Array("testJob"), Some(DEFAULT_CONFIG_STR))

  def getCheckResults[IN <: ADT, OUT <: ADT, ADT <: FlinkEvent](
      checkResultsName: String,
      tests: List[OUT] => Unit,
      input: List[IN] = List.empty
  ): CheckResults[ADT] = {
    new CheckResults[ADT] {
      override val name: String = checkResultsName

      override val collectLimit: Int =
        if (input.nonEmpty) input.size else 100

      override def getInputEvents[T <: ADT](sourceName: String): List[T] =
        if (input.isEmpty) super.getInputEvents(sourceName)
        else input.asInstanceOf[List[T]]

      override def checkOutputEvents[T <: ADT](out: List[T]): Unit =
        tests(out.asInstanceOf[List[OUT]])
    }
  }

  def getRunner[
      IN <: ADT: TypeInformation,
      OUT <: ADT: TypeInformation,
      JF <: StreamJob[OUT, ADT],
      ADT <: FlinkEvent: TypeInformation](
      configStr: String = DEFAULT_CONFIG_STR,
      jobFactory: JobFactory[JF, IN, OUT, ADT],
      checkResultsOpt: Option[CheckResults[ADT]] = None,
      args: Array[String] = Array("testJob"),
      executeJob: Boolean = true): FlinkRunner[ADT] = {
    val config = new FlinkConfig(args, Some(configStr))
    new FlinkRunner[ADT](
      config,
      checkResultsOpt = checkResultsOpt,
      executeJob = executeJob
    ) {
      override def invoke(jobName: String): Unit =
        jobFactory.getJob(this).run()
    }
  }

  def getStreamJobRunner[
      IN <: ADT: TypeInformation,
      OUT <: ADT: TypeInformation,
      ADT <: FlinkEvent: TypeInformation](
      configStr: String = DEFAULT_CONFIG_STR,
      transformer: MapFunction[IN, OUT],
      input: Seq[IN] = Seq.empty,
      sourceName: Option[String] = None,
      checkResultsOpt: Option[CheckResults[ADT]] = None,
      args: Array[String] = Array("testJob"),
      executeJob: Boolean = true): FlinkRunner[ADT] = {
    getRunner(
      configStr,
      new StreamJobFactory[IN, OUT, ADT](transformer, input, sourceName),
      checkResultsOpt,
      args,
      executeJob
    )
  }

  def getIdentityStreamJobRunner[
      OUT <: ADT: TypeInformation,
      ADT <: FlinkEvent: TypeInformation](
      configStr: String = DEFAULT_CONFIG_STR,
      input: Seq[OUT] = Seq.empty,
      sourceName: Option[String] = None,
      checkResultsOpt: Option[CheckResults[ADT]] = None,
      args: Array[String] = Array("testJob"),
      executeJob: Boolean = true): FlinkRunner[ADT] =
    getRunner(
      configStr,
      new IdentityStreamJobFactory(input, sourceName),
      checkResultsOpt,
      args,
      executeJob
    )

  def getAvroStreamJobRunner[
      IN <: ADT with EmbeddedAvroRecord[INA]: TypeInformation,
      INA <: GenericRecord: TypeInformation,
      OUT <: ADT with EmbeddedAvroRecord[OUTA]: TypeInformation,
      OUTA <: GenericRecord: TypeInformation,
      ADT <: FlinkEvent: TypeInformation](
      configStr: String = DEFAULT_CONFIG_STR,
      transformer: MapFunction[IN, OUT],
      input: Seq[IN] = Seq.empty,
      sourceName: Option[String] = None,
      checkResultsOpt: Option[CheckResults[ADT]] = None,
      args: Array[String] = Array("testJob"),
      executeJob: Boolean = true)(implicit
      fromKV: EmbeddedAvroRecordInfo[INA] => IN): FlinkRunner[ADT] =
    getRunner(
      configStr,
      new AvroStreamJobFactory[IN, INA, OUT, OUTA, ADT](
        transformer,
        input,
        sourceName
      ),
      checkResultsOpt,
      args,
      executeJob
    )

  def getIdentityAvroStreamJobRunner[
      OUT <: ADT with EmbeddedAvroRecord[OUTA]: TypeInformation,
      OUTA <: GenericRecord: TypeInformation,
      ADT <: FlinkEvent: TypeInformation](
      configStr: String = DEFAULT_CONFIG_STR,
      input: Seq[OUT] = Seq.empty,
      sourceName: Option[String] = None,
      checkResultsOpt: Option[CheckResults[ADT]] = None,
      args: Array[String] = Array("testJob"),
      executeJob: Boolean = true)(implicit
      fromKV: EmbeddedAvroRecordInfo[OUTA] => OUT): FlinkRunner[ADT] =
    getRunner(
      configStr,
      new IdentityAvroStreamJobFactory[OUT, OUTA, ADT](input, sourceName),
      checkResultsOpt,
      args,
      executeJob
    )

  def getTableStreamJobRunner[
      IN <: ADT with EmbeddedRowType: TypeInformation: ru.TypeTag,
      OUT <: ADT with EmbeddedRowType: TypeInformation: ru.TypeTag,
      ADT <: FlinkEvent: TypeInformation](
      configStr: String = DEFAULT_CONFIG_STR,
      transformer: MapFunction[IN, OUT],
      input: Seq[IN] = Seq.empty,
      sourceName: Option[String] = None,
      checkResultsOpt: Option[CheckResults[ADT]] = None,
      args: Array[String] = Array("testJob"),
      executeJob: Boolean = true)(implicit
      fromRowData: RowData => IN): FlinkRunner[ADT] =
    getRunner(
      configStr,
      new TableStreamJobFactory[IN, OUT, ADT](
        transformer,
        input,
        sourceName
      ),
      checkResultsOpt,
      args,
      executeJob
    )

  def getIdentityTableStreamJobRunner[
      OUT <: ADT with EmbeddedRowType: TypeInformation: ru.TypeTag,
      ADT <: FlinkEvent: TypeInformation](
      configStr: String = DEFAULT_CONFIG_STR,
      input: Seq[OUT] = Seq.empty,
      sourceName: Option[String] = None,
      checkResultsOpt: Option[CheckResults[ADT]] = None,
      args: Array[String] = Array("testJob"),
      executeJob: Boolean = true)(implicit
      fromRowData: RowData => OUT): FlinkRunner[ADT] =
    getRunner(
      configStr,
      new IdentityTableStreamJobFactory[OUT, ADT](input, sourceName),
      checkResultsOpt,
      args,
      executeJob
    )
}
