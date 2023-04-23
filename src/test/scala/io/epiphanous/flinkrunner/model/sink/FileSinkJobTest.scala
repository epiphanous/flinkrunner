package io.epiphanous.flinkrunner.model.sink

import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.model._
import org.apache.flink.api.scala.createTypeInformation

class FileSinkJobTest extends PropSpec {

  property("write avro json results to sink") {
    val input     = genPop[BWrapper]()
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
    getIdentityAvroStreamJobRunner[BWrapper, BRecord, MyAvroADT](
      configStr,
      input
    ).process()
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
    getIdentityAvroStreamJobRunner[BWrapper, BRecord, MyAvroADT](
      configStr,
      genPop[BWrapper]()
    ).process()
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
    getIdentityStreamJobRunner[SimpleA, MySimpleADT](
      configStr,
      genPop[SimpleA]()
    ).process()
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
    getIdentityStreamJobRunner[SimpleA, MySimpleADT](
      configStr,
      genPop[SimpleA]()
    ).process()
  }

}
