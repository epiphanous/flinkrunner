package io.epiphanous.flinkrunner.model.sink

import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.{FlinkRunner, FlinkRunnerSpec}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation

class SinkSpec extends FlinkRunnerSpec with AvroFileTestUtils {

  def getFactory[E <: MySimpleADT: TypeInformation]
      : FlinkRunner[MySimpleADT] => SimpleIdentityJob[E] =
    (runner: FlinkRunner[MySimpleADT]) => new SimpleIdentityJob[E](runner)

  def getAvroFactory[
      E <: MyAvroADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](implicit
      fromKV: EmbeddedAvroRecordInfo[A] => E)
      : FlinkRunner[MyAvroADT] => SimpleAvroIdentityJob[
        E,
        A
      ] = (runner: FlinkRunner[MyAvroADT]) =>
    new SimpleAvroIdentityJob[E, A](runner)

  def getJobConfig(
      sinkConfigStr: String,
      sourceFile: String,
      sourceFormat: String = "csv",
      batchMode: Boolean = true,
      otherJobConfig: String = ""): String =
    s"""
       |${if (batchMode) "runtime.execution-mode=batch" else ""}
       |jobs {
       |  testJob {
       |    $otherJobConfig
       |    sources {
       |      file-source {
       |        path = "$sourceFile"
       |        format = $sourceFormat
       |      }
       |    }
       |    sinks {
       |      $sinkConfigStr
       |    }
       |  }
       |}
       |""".stripMargin

  def testJob[E <: MySimpleADT: TypeInformation](
      sinkConfigStr: String,
      sourceFile: String,
      sourceFormat: String = "csv",
      batchMode: Boolean = true,
      otherJobConfig: String = "runtime.execution-mode = batch"): Unit =
    testStreamJob(
      getJobConfig(
        sinkConfigStr,
        sourceFile,
        sourceFormat,
        batchMode,
        otherJobConfig
      ),
      getFactory[E]
    )

  def testAvroJob[
      E <: MyAvroADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      sinkConfigStr: String,
      sourceFile: String,
      sourceFormat: String = "csv",
      batchMode: Boolean = true,
      otherJobConfig: String = "")(implicit
      fromKV: EmbeddedAvroRecordInfo[A] => E): Unit =
    testAvroStreamJob(
      getJobConfig(
        sinkConfigStr,
        sourceFile,
        sourceFormat,
        batchMode,
        otherJobConfig
      ),
      getAvroFactory[E, A]
    )

}
