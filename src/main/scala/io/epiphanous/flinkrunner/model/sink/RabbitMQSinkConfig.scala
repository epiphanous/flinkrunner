package io.epiphanous.flinkrunner.model.sink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.serde.{
  EmbeddedAvroJsonSerializationSchema,
  JsonSerializationSchema
}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.rabbitmq.{
  RMQSink,
  RMQSinkPublishOptions
}

case class RabbitMQSinkConfig[ADT <: FlinkEvent: TypeInformation](
    name: String,
    config: FlinkConfig)
    extends SinkConfig[ADT]
    with LazyLogging {

  override val connector: FlinkConnectorName = FlinkConnectorName.RabbitMQ

  val uri: String                            = config.getString(pfx("uri"))
  val useCorrelationId: Boolean              =
    config.getBoolean(pfx("use.correlation.id"))
  val connectionInfo: RabbitMQConnectionInfo =
    RabbitMQConnectionInfo(uri, properties)
  val queue: Option[String]                  = config.getStringOpt(pfx("queue"))

  def getSerializationSchema[E <: ADT: TypeInformation]
      : SerializationSchema[E] = new JsonSerializationSchema[E, ADT](this)

  def getAvroSerializationSchema[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation] =
    new EmbeddedAvroJsonSerializationSchema[E, A, ADT](this)

  def getPublishOptions[E <: ADT: TypeInformation]
      : Option[RMQSinkPublishOptions[E]] = None

  def _getSink[E <: ADT: TypeInformation](
      dataStream: DataStream[E],
      serializationSchema: SerializationSchema[E]): DataStreamSink[E] = {
    val connConfig = connectionInfo.rmqConfig

    val sink = getPublishOptions[E] match {
      case Some(p) => new RMQSink(connConfig, serializationSchema, p)
      case None    =>
        queue match {
          case Some(q) => new RMQSink(connConfig, q, serializationSchema)
          case None    =>
            throw new RuntimeException(
              s"RabbitMQ sink $name config requires either a queue name or publishing options"
            )
        }
    }
    dataStream
      .addSink(sink)
      .uid(label)
      .name(label)
      .setParallelism(parallelism)
  }

  override def getSink[E <: ADT: TypeInformation](
      dataStream: DataStream[E]): DataStreamSink[E] =
    _getSink[E](dataStream, getSerializationSchema[E])

  override def getAvroSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      dataStream: DataStream[E]): DataStreamSink[E] =
    _getSink[E](dataStream, getAvroSerializationSchema[E, A])

}
