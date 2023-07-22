package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model.sink.SinkConfig
import io.epiphanous.flinkrunner.model.{MySimpleADT, SimpleA}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag}
import org.apache.flink.util.Collector

class TestSideSinkJob(
    runner: FlinkRunner[MySimpleADT],
    input: Seq[SimpleA])
    extends StreamJob[SimpleA, MySimpleADT](runner) {

  val sideSinkConfig: SinkConfig[MySimpleADT] =
    runner.getSinkConfig("side-test-list-sink")

  val outputTag: OutputTag[SimpleA] = sideSinkConfig.getOutputTag[SimpleA]

  override def transform: DataStream[SimpleA] =
    runner.env
      .fromCollection(input)
      .process(new MyProcessFunction(outputTag))

  override def sinkSideOutputs(out: DataStream[SimpleA]): Unit =
    sideSinkConfig.addSink(out.getSideOutput(outputTag))
}

class MyProcessFunction(outputTag: OutputTag[SimpleA])
    extends ProcessFunction[SimpleA, SimpleA] {
  override def processElement(
      element: SimpleA,
      ctx: ProcessFunction[SimpleA, SimpleA]#Context,
      out: Collector[SimpleA]): Unit = {
    out.collect(element)
    ctx.output(
      outputTag,
      element.copy(id = s"side-${element.id}", a1 = element.a1 * 2)
    )
  }
}
