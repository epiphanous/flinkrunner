package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.{FlinkRunner, PropSpec}
import io.epiphanous.flinkrunner.model.{
  ARecord,
  AWrapper,
  BWrapper,
  FlinkConfig,
  MyAvroADT,
  MySimpleADT,
  SimpleA,
  SimpleB,
  SimpleC
}
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

class StreamJobTest extends PropSpec {

  property("singleSource") {
    val in     = genPop[BWrapper](10)
    val outLen = in.count(_.value.b2.nonEmpty)
    val config = new FlinkConfig(
      Array("singleSource"),
      Some("""
        |environment = test
        |mock.edges = true
        |""".stripMargin)
    )
    new FlinkRunner[MyAvroADT](
      config,
      Map("in" -> in),
      out => {
        println("========================================")
        out.foreach(println)
        out.length shouldEqual outLen
        out.forall(_.isInstanceOf[AWrapper]) shouldBe true
      }
    ) {
      override def invoke(
          jobName: String): Either[List[_], JobExecutionResult] = {
        jobName match {
          case "singleSource" => new SingleSourceTestJob(this).run()
        }
      }
    }.process()
  }
}

class SingleSourceTestJob(runner: FlinkRunner[MyAvroADT])
    extends StreamJob[AWrapper, MyAvroADT](runner) {
  override def transform: DataStream[AWrapper] =
    singleSource[BWrapper]().flatMap { bw =>
      val b = bw.value
      b.b2
        .map(d =>
          List(AWrapper(ARecord(b.b0, b.b1.getOrElse(1), d, b.b3)))
        )
        .getOrElse(List.empty)
    }
}

class BroadcastConnectedTestJob(runner: FlinkRunner[MySimpleADT])
    extends StreamJob[SimpleC, MySimpleADT](runner) {
  override def transform: DataStream[SimpleC] =
    broadcastConnectedSource[SimpleA, SimpleB]("a", "b").process(
      new KeyedBroadcastProcessFunction[
        String,
        SimpleA,
        SimpleB,
        SimpleC] {
        override def processElement(
            value: SimpleA,
            ctx: KeyedBroadcastProcessFunction[
              String,
              SimpleA,
              SimpleB,
              SimpleC]#ReadOnlyContext,
            out: Collector[SimpleC]): Unit = ???

        override def processBroadcastElement(
            value: SimpleB,
            ctx: KeyedBroadcastProcessFunction[
              String,
              SimpleA,
              SimpleB,
              SimpleC]#Context,
            out: Collector[SimpleC]): Unit = ???
      }
    )
}

class FilterByControlTestJob(runner: FlinkRunner[MySimpleADT])
    extends StreamJob[SimpleB, MySimpleADT](runner) {
  override def transform: DataStream[SimpleB] =
    filterByControlSource[SimpleA, SimpleB]("a", "b")
}
