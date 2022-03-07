package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.{FlinkRunner, PropSpec}
import io.epiphanous.flinkrunner.model.{
  ARecord,
  AWrapper,
  BWrapper,
  MyAvroADT,
  MySimpleADT,
  SimpleA,
  SimpleB,
  SimpleC
}
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

class StreamJobTest extends PropSpec {

  property("singleSource") {}
}

class SingleSourceTestJob(runner: FlinkRunner[MyAvroADT])
    extends StreamJob[AWrapper, MyAvroADT](runner) {
  override def transform: DataStream[AWrapper] =
    singleSource[BWrapper](Some("dog")).flatMap { bw =>
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