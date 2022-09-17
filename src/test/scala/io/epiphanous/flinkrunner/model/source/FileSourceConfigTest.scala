package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.{FlinkRunner, PropSpec}
import io.epiphanous.flinkrunner.flink.AvroStreamJob
import io.epiphanous.flinkrunner.model.{
  BRecord,
  BWrapper,
  CheckResults,
  MyAvroADT
}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.DataStream

class FileSourceConfigTest extends PropSpec {

  class TestCheckResults extends CheckResults[MyAvroADT] {
    override val name: String = "test-check-results"

    override def checkOutputEvents[OUT <: MyAvroADT](
        out: List[OUT]): Unit = {
//      println(out.map(_.toString).mkString("\n"))
      out.length shouldEqual 20
      out.head.asInstanceOf[BWrapper].$record.b1 shouldEqual Some(
        825007375
      )
    }
  }

  class TestIdentityJob(runner: FlinkRunner[MyAvroADT])
      extends AvroStreamJob[BWrapper, BRecord, MyAvroADT](runner) {
    override def transform: DataStream[BWrapper] =
      singleAvroSource[BWrapper, BRecord]()
  }

  property("getAvroSourceStream Avro format property") {
    val optConfig =
      """
        |execution.runtime-mode = batch
        |jobs {
        |  test-job {
        |    sources {
        |      test-file-source {
        |        path = "resource://avro-test/"
        |        format = avro
        |      }
        |    }
        |  }
        |}
        |""".stripMargin
    getAvroJobRunner[TestIdentityJob, BWrapper, BRecord, MyAvroADT](
      Array("test-job"),
      optConfig,
      new TestCheckResults,
      (_, runner) => new TestIdentityJob(runner)
    ).process()
  }
}
