package io.epiphanous.flinkrunner.model.sink

import io.epiphanous.flinkrunner.flink.{AvroStreamJob, StreamJob}
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.{FlinkRunner, FlinkRunnerSpec}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.DataStream

class FileSinkJobTest extends FlinkRunnerSpec {

  class IdentityJob(runner: FlinkRunner[MySimpleADT], input: Seq[SimpleB])
      extends StreamJob[SimpleB, MySimpleADT](runner) {
    override def transform: DataStream[SimpleB] =
      runner.env.fromCollection(input)
  }

  class IdentityAvroJob(
      runner: FlinkRunner[MyAvroADT],
      input: Seq[BWrapper])
      extends AvroStreamJob[BWrapper, BRecord, MyAvroADT](runner) {
    override def transform: DataStream[BWrapper] =
      runner.env.fromCollection(input)
  }

  property("write avro json results to sink") {
    val configStr =
      """
        |sinks {
        |  test-file-sink {
        |    path = "/tmp/json-avro-output"
        |    format = json
        |    config {
        |      output.file.part.suffix = ".json"
        |    }
        |  }
        |}
        |job testJob {
        |}
        |execution.runtime-mode = batch
        |""".stripMargin
    val input     = genPop[BWrapper]()
    val factory   =
      (runner: FlinkRunner[MyAvroADT]) =>
        new IdentityAvroJob(runner, input)
    testAvroStreamJob(configStr, factory)

  }

  property("write avro delimited results to sink") {
    val configStr =
      """
        |sinks {
        |  test-file-sink {
        |    path = "/tmp/csv-avro-output"
        |    format = csv
        |    config {
        |      output.file.part.suffix = ".csv"
        |    }
        |  }
        |}
        |job testJob {
        |}
        |execution.runtime-mode = batch
        |""".stripMargin
    val input     = genPop[BWrapper]()
    val factory   =
      (runner: FlinkRunner[MyAvroADT]) =>
        new IdentityAvroJob(runner, input)
    testAvroStreamJob(configStr, factory)
  }

  property("write simple json results to sink") {
    val configStr =
      """
        |sinks {
        |  test-file-sink {
        |    path = "/tmp/json-simple-output"
        |    format = json
        |    config {
        |      output.file.part.suffix = ".json"
        |    }
        |  }
        |}
        |job testJob {
        |}
        |execution.runtime-mode = batch
        |""".stripMargin
    val input     = genPop[SimpleB]()
    val factory   =
      (runner: FlinkRunner[MySimpleADT]) => new IdentityJob(runner, input)
    testStreamJob(configStr, factory)
  }

  property("write simple delimited results to sink") {
    val configStr =
      """
        |sinks {
        |  test-file-sink {
        |    path = "/tmp/csv-simple-output"
        |    format = csv
        |    config {
        |      output.file.part.suffix = ".csv"
        |    }
        |  }
        |}
        |job testJob {
        |}
        |execution.runtime-mode = batch
        |""".stripMargin
    val input     = genPop[SimpleB]()
    val factory   =
      (runner: FlinkRunner[MySimpleADT]) => new IdentityJob(runner, input)
    testStreamJob(configStr, factory)
  }

}
