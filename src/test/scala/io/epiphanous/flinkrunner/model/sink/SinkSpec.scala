package io.epiphanous.flinkrunner.model.sink

import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.model._
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation

class SinkSpec extends PropSpec with AvroFileTestUtils {

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
      otherJobConfig: String = "runtime.execution-mode = batch"): Unit = {
    getIdentityStreamJobRunner[E, MySimpleADT](
      getJobConfig(
        sinkConfigStr,
        sourceFile,
        sourceFormat,
        batchMode,
        otherJobConfig
      )
    ).process()
  }

  def testAvroJob[
      E <: MyAvroADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      sinkConfigStr: String,
      sourceFile: String,
      sourceFormat: String = "csv",
      batchMode: Boolean = true,
      otherJobConfig: String = "")(implicit
      fromKV: EmbeddedAvroRecordInfo[A] => E): Unit =
    getIdentityAvroStreamJobRunner[E, A, MyAvroADT](
      getJobConfig(
        sinkConfigStr,
        sourceFile,
        sourceFormat,
        batchMode,
        otherJobConfig
      )
    ).process()

}
