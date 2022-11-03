package io.epiphanous.flinkrunner.flink.state

import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector

object ListStateTest {

  def main(args: Array[String]): Unit = {

    /*
      This local job test should yield:

      State is empty for element [1]!
      State is empty for element [2]!
      State is empty for element [3]!
      State is empty for element [4]!
      State contains 1
      State contains 2
      State contains 3
      State contains 4
     */

    val env = StreamExecutionEnvironment.createLocalEnvironment(1)

    val dataStream : DataStreamSource[Integer] = env.addSource(new SourceFunction[Integer] {
      override def run(ctx: SourceFunction.SourceContext[Integer]): Unit = {
        for( x <- 1 until 3) {
          val data = List.fill(1)(List(1, 2, 3, 4)).flatten
          for (x <- data) {
            ctx.collect(new Integer(x))
          }
        }
      }

      override def cancel(): Unit = ???
    })

    dataStream.keyBy(new KeySelector[Integer, Integer] {
      override def getKey(value: Integer): Integer = {
        value
      }
    }).process(new ProcessFunction[Integer, Integer] {

      val listState = new ListState[Integer]()
      override def open(parameters: Configuration): Unit = {
        this.listState(getRuntimeContext.getListState(new ListStateDescriptor[Integer](
          "foo",
          createTypeInformation[Integer]
        )))
      }
      override def processElement(value: Integer, ctx: ProcessFunction[Integer, Integer]#Context, out: Collector[Integer]): Unit = {

        if(this.listState.isEmpty) {
          println(s"State is empty for element [$value]!")
        }

        if(this.listState.contains(value)) {
          println(s"State contains $value")
        }

        listState.add(value)
      }
    })

    env.execute("ListStateTestLocalJob")

  }

}
