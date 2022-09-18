package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.flink.AvroStreamJob
import io.epiphanous.flinkrunner.model.{
  AvroFileTestUtils,
  BRecord,
  BWrapper,
  CheckResults,
  MyAvroADT
}
import io.epiphanous.flinkrunner.{FlinkRunner, PropSpec}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.DataStream

import java.nio.file.{Files, Paths}
import scala.util.Try

class FileSourceConfigTest extends PropSpec with AvroFileTestUtils {

  class TestCheckResults(pop: List[BWrapper], path: java.nio.file.Path)
      extends CheckResults[MyAvroADT] {
    override val name: String = "test-check-results"

    override def checkOutputEvents[OUT <: MyAvroADT](
        out: List[OUT]): Unit = {
      println("=====[INPUT]======\n" + pop.mkString("\n"))
      println("\n=====[OUTPUT]======\n" + out.mkString("\n"))
      Try(Files.delete(path)) should be a 'success
      out shouldEqual pop
      ()
    }
  }

  class TestIdentityJob(runner: FlinkRunner[MyAvroADT])
      extends AvroStreamJob[BWrapper, BRecord, MyAvroADT](runner) {
    override def transform: DataStream[BWrapper] =
      singleAvroSource[BWrapper, BRecord]()
  }

  def doAvroFileSourceTest(isParquet: Boolean) = {
    val format = if (isParquet) "parquet" else "avro"
    val pop    = genPop[BWrapper]()
    getTempFile(isParquet).map { path =>
      val file      = path.toString
      writeFile(file, isParquet, pop)
      val optConfig =
        s"""
           |execution.runtime-mode = batch
           |jobs {
           |  $format-test-job {
           |    sources {
           |      test-file-source {
           |        path = "$file"
           |        format = $format
           |      }
           |    }
           |  }
           |}
           |""".stripMargin
      getAvroJobRunner[TestIdentityJob, BWrapper, BRecord, MyAvroADT](
        Array(s"$format-test-job"),
        optConfig,
        new TestCheckResults(pop, path),
        (_, runner) => new TestIdentityJob(runner)
      ).process()
    }
  }

  property("getAvroSourceStream Avro format property") {
    doAvroFileSourceTest(false)
  }

  property("getAvroSourceStream Parquet format property") {
    doAvroFileSourceTest(true)
  }

}
