package io.epiphanous.flinkrunner.model.sink

import io.epiphanous.flinkrunner.model.FlinkConnectorName.DynamoDb
import io.epiphanous.flinkrunner.model.{
  EmbeddedAvroRecord,
  FlinkConfig,
  FlinkConnectorName,
  FlinkEvent
}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.dynamodb.sink.{
  DynamoDbSink,
  DynamoDbSinkBuilder
}
import org.apache.flink.streaming.api.scala.DataStream

case class DynamoDbSinkConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig)
    extends SinkConfig[ADT] {
  override def connector: FlinkConnectorName = DynamoDb

  def _getSink[E <: ADT: TypeInformation]: DynamoDbSink[E] = {
    val builder = new DynamoDbSinkBuilder[E]()
    builder.build()
  }

  override def addSink[E <: ADT: TypeInformation](
      stream: DataStream[E]): Unit = stream.addSink[E](_getSink)

  override def addAvroSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](stream: DataStream[E]): Unit =
    ???

}
