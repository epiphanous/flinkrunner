package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.model.sink.TestListSinkConfig
import io.epiphanous.flinkrunner.model.{FlinkConfig, MySimpleADT, SimpleA}
import io.epiphanous.flinkrunner.{FlinkRunner, PropSpec}
import org.apache.flink.api.scala._

class SideOutputSpec extends PropSpec {

  property("side sink configs seen") {
    getIdentityStreamJobRunner[
      SimpleA,
      MySimpleADT
    ]().sideSinkConfigs.isEmpty shouldBe true
    getIdentityStreamJobRunner[SimpleA, MySimpleADT](s"""
         |jobs {
         |  testJob {
         |    sources { empty-sink {} }
         |    sinks {
         |      print-sink {}
         |      side-print-sink { side.output = true }
         |    }
         |  }
         |}
         |""".stripMargin).sideSinkConfigs.size shouldBe 1
    getIdentityStreamJobRunner[SimpleA, MySimpleADT](s"""
         |jobs {
         |  testJob {
         |    sources { empty-sink {} }
         |    sinks {
         |      print-sink {}
         |      first-side-print-sink { side.output = true }
         |      second-side-print-sink { side.output = true }
         |    }
         |  }
         |}
         |""".stripMargin).sideSinkConfigs.size shouldBe 2
  }

  property("side sinks emit outputs") {
    val config         = new FlinkConfig(
      Array("testJob"),
      Some(s"""
         |execution.runtime-mode = batch
         |jobs {
         |  testJob {
         |    sinks {
         |      main-print-sink {}
         |      side-test-list-sink {
         |        side.output = true
         |      }
         |    }
         |  }
         |}
         |""".stripMargin)
    )
    val input          = genPop[SimpleA](3).sorted
    val runner         = new MyTestRunner(config, input)
    runner.process()
    val testSinkConfig = runner
      .getSinkConfig("side-test-list-sink")
      .asInstanceOf[TestListSinkConfig[MySimpleADT]]
    val sideResults    = testSinkConfig.getSortedResults[SimpleA]
    sideResults.zip(input).foreach { case (side, orig) =>
      side.id shouldEqual s"side-${orig.id}"
      side.a1 shouldEqual orig.a1 * 2
    }
  }

}

class MyTestRunner(config: FlinkConfig, input: Seq[SimpleA])
    extends FlinkRunner[MySimpleADT](config) {
  override def invoke(jobName: String): Unit =
    new TestSideSinkJob(this, input).run()
}
