package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.flink.AvroStreamJob
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.{FlinkRunner, PropSpec}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.DataStream

import java.nio.file.Files
import scala.util.Try

class FileSourceConfigTest extends PropSpec with AvroFileTestUtils {

  class TestCheckResults(in: List[BWrapper], path: java.nio.file.Path)
      extends CheckResults[MyAvroADT] {
    override val name: String = "test-check-results"

    override def checkOutputEvents[OUT <: MyAvroADT](
        out: List[OUT]): Unit = {
      println("=====[INPUT]======\n" + in.mkString("\n"))
      println("\n=====[OUTPUT]======\n" + out.mkString("\n"))
      Try(Files.delete(path)) should be a 'success
      out shouldEqual in
      ()
    }
  }

  class TestIdentityJob(runner: FlinkRunner[MyAvroADT])
      extends AvroStreamJob[BWrapper, BRecord, MyAvroADT](runner) {
    override def transform: DataStream[BWrapper] =
      singleAvroSource[BWrapper, BRecord]()
  }

  def doFileSourceTest(format: StreamFormatName) = {
    val fmtName = format.entryName.toLowerCase
    val in      = genPop[BWrapper]()
    getTempFile(format).map { path =>
      val file      = path.toString
      writeFile(file, format, in)
      val optConfig =
        s"""
           |execution.runtime-mode = batch
           |jobs {
           |  $fmtName-test-job {
           |    sources {
           |      test-file-source {
           |        path = "$file"
           |        format = $fmtName
           |      }
           |    }
           |  }
           |}
           |""".stripMargin
      getAvroJobRunner[TestIdentityJob, BWrapper, BRecord, MyAvroADT](
        Array(s"$fmtName-test-job"),
        optConfig,
        new TestCheckResults(in, path),
        (_, runner) => new TestIdentityJob(runner)
      ).process()
    }
  }

  property("getAvroSourceStream Avro format property") {
    doFileSourceTest(StreamFormatName.Avro)
  }

  property("getAvroSourceStream Parquet format property") {
    doFileSourceTest(StreamFormatName.Parquet)
  }

  property("getAvroSourceStream Json format property") {
    doFileSourceTest(StreamFormatName.Json)
  }

  property("getAvroSourceStream Csv format property") {
    doFileSourceTest(StreamFormatName.Csv)
  }

}
