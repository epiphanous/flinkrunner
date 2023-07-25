package io.epiphanous.flinkrunner.model.sink

import io.epiphanous.flinkrunner.model.FlinkConnectorName.TestList
import io.epiphanous.flinkrunner.model.{
  EmbeddedAvroRecord,
  FlinkConfig,
  FlinkConnectorName,
  FlinkEvent
}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.test.streaming.runtime.util.TestListResultSink

import scala.collection.JavaConverters._

case class TestListSinkConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig)
    extends SinkConfig[ADT] {

  override val connector: FlinkConnectorName = TestList

  var testSink: Option[TestListResultSink[_ <: ADT]] = None

  def getResults[E <: ADT]: List[E] =
    testSink
      .map(_.getResult.asScala.toList.asInstanceOf[List[E]])
      .getOrElse(List.empty[E])

  def getSortedResults[E <: ADT]: List[E] =
    testSink
      .map(_.getSortedResult.asScala.toList.asInstanceOf[List[E]])
      .getOrElse(List.empty[E])

  def __addSink[E <: ADT: TypeInformation](stream: DataStream[E]): Unit = {
    val s = new TestListResultSink[E]
    stream.addSink(s)
    testSink = Some(s)
    ()
  }

  override def addSink[E <: ADT: TypeInformation](
      stream: DataStream[E]): Unit =
    __addSink[E](stream)

  override def addAvroSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](stream: DataStream[E]): Unit =
    __addSink[E](stream)

}
