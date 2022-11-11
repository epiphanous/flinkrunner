package io.epiphanous.flinkrunner.flink.state

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util
import java.util.Collections
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector
import RichStateUtils._

class ListStateTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setNumberSlotsPerTaskManager(2)
    .setNumberTaskManagers(1)
    .build)

  before {
    flinkCluster.before()
  }

  after {
    flinkCluster.after()
  }


  "ListStateTest1 pipeline" should "group correctly" in {

    CollectSink.values.clear()

    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment()

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

      var listState: org.apache.flink.api.common.state.ListState[Integer] = _

      override def open(parameters: Configuration): Unit = {
        this.listState = getRuntimeContext.getListState(new ListStateDescriptor[Integer](
          "foo",
          createTypeInformation[Integer]
        ))
      }
      override def processElement(value: Integer, ctx: ProcessFunction[Integer, Integer]#Context, out: Collector[Integer]): Unit = {

        if(this.listState.isEmpty) {
          println(s"State is empty for element [$value]!")
        }

        if(this.listState.contains(value)) {
          println(s"State contains $value")
          out.collect(value + 10)
        }
        listState.add(value)
      }
    }).addSink(new CollectSink())

    env.execute("ListStateTestLocalJob")
    CollectSink.values should contain allOf(11, 12, 13, 14)
  }
}

class CollectSink extends SinkFunction[Integer] {
  override def invoke(value: Integer, context: SinkFunction.Context): Unit = {
    CollectSink.values.add(value)
  }
}
object CollectSink {
  val values: util.List[Integer] = Collections.synchronizedList(new util.ArrayList())
}