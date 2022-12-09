package io.epiphanous.flinkrunner.flink.state

import io.epiphanous.flinkrunner.UnitSpec
import io.epiphanous.flinkrunner.flink.state.RichStateUtils._
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.flink.util.Collector
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers

import java.util
import java.util.Collections
class RichStateUtilsTests
    extends UnitSpec
    with Matchers
    with BeforeAndAfter {

  val flinkCluster = new MiniClusterWithClientResource(
    new MiniClusterResourceConfiguration.Builder()
      .setNumberSlotsPerTaskManager(2)
      .setNumberTaskManagers(1)
      .build
  )

  before {
    flinkCluster.before()
  }

  after {
    flinkCluster.after()
  }

  "RichListStateTest1 pipeline" should "implement all members" in {

    CollectSink.values.clear()

    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment()

    val dataStream: DataStreamSource[Integer] =
      env.addSource(new SourceFunction[Integer] {
        override def run(
            ctx: SourceFunction.SourceContext[Integer]): Unit = {
          for (x <- 1 until 3) {
            val data = List.fill(1)(List(1, 2, 3, 4)).flatten
            for (x <- data)
              ctx.collect(new Integer(x))
          }
        }

        override def cancel(): Unit = ???
      })

    dataStream
      .keyBy(new KeySelector[Integer, Integer] {
        override def getKey(value: Integer): Integer =
          value
      })
      .process(new ProcessFunction[Integer, Integer] {

        var listState
            : org.apache.flink.api.common.state.ListState[Integer] = _

        override def open(parameters: Configuration): Unit = {
          this.listState = getRuntimeContext.getListState(
            new ListStateDescriptor[Integer](
              "foo",
              createTypeInformation[Integer]
            )
          )
        }
        override def processElement(
            value: Integer,
            ctx: ProcessFunction[Integer, Integer]#Context,
            out: Collector[Integer]): Unit = {

          // These groups of related functions should yield functionality equivalent results
          assert(
            (this.listState.isEmpty && this.listState.length <= 0)
              ||
                (!this.listState.isEmpty && this.listState.length > 0)
          )

          assert(
            (this.listState.contains(value) && this.listState
              .find(value)
              .get
              .equals(value))
              ||
                (!this.listState
                  .contains(value) && this.listState.find(value).isEmpty)
          )

          if (!this.listState.isEmpty && this.listState.contains(value)) {
            // collects (2, 4, 6, 8) [ yields 4 elements ]
            out.collect(value * 2)
            // collects (1, 2, 3, 4) [ yields 4 elements ]
            out.collect(this.listState.find(value).get)
          }
          listState.add(value)
        }
      })
      .addSink(new CollectSink())

    env.execute("ListStateTestLocalJob")
    CollectSink.values should contain allOf (1, 2, 3, 4, 6, 8)
  }
}

private class CollectSink extends SinkFunction[Integer] {
  override def invoke(
      value: Integer,
      context: SinkFunction.Context): Unit =
    CollectSink.values.add(value)
}
private object CollectSink {
  val values: util.List[Integer] =
    Collections.synchronizedList(new util.ArrayList())
}
