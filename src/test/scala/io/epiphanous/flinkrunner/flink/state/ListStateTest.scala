package io.epiphanous.flinkrunner.flink.state

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInfo
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{createTypeInformation}
import org.apache.flink.util.Collector

object ListStateTest {

  def main(args: Array[String]): Unit = {

    val dummySourceSeq =  1 to 10 by 1
    val env = StreamExecutionEnvironment.createLocalEnvironment(1)
    val dataStream : DataStreamSource[List[Int]] = env.fromElements(List.fill(10000)(dummySourceSeq.toList))

    val d = dataStream.flatMap(new FlatMapFunction[List[Int], Int] {
      override def flatMap(value: List[Int], out: Collector[Int]): Unit = {
        value.foreach(x => out.collect(x))
      }
    }).keyBy(new KeySelector[Int, Int] {
      override def getKey(value: Int): Int = {
        value
      }
    }).process(new ProcessFunction[Int, Int] {

      val listState = new ListState[Int]()
      override def open(parameters: Configuration): Unit = {
        val d = new ListStateDescriptor[Int](
          "foo",
          createTypeInformation[Int]
        )

        this.listState(getRuntimeContext.getListState(d))
      }
      override def processElement(value: Int, ctx: ProcessFunction[Int, Int]#Context, out: Collector[Int]): Unit = {
        if(this.listState.isEmpty) {
          println("State is empty!")
          listState.state.add(value)
        }

        if(this.listState.contains(value)) {
          println("State contains")
        }
      }
    })



  }

}
