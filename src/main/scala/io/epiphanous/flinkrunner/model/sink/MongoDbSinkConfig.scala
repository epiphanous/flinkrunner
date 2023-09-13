package io.epiphanous.flinkrunner.model.sink

import com.mongodb.client.model.{InsertOneModel, WriteModel}
import io.epiphanous.flinkrunner.model.FlinkConnectorName.MongoDb
import io.epiphanous.flinkrunner.model.{
  EmbeddedAvroRecord,
  FlinkConfig,
  FlinkConnectorName,
  FlinkEvent
}
import io.epiphanous.flinkrunner.serde.{
  EmbeddedAvroJsonFileEncoder,
  JsonFileEncoder
}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.mongodb.sink.writer.context.MongoSinkContext
import org.apache.flink.connector.mongodb.sink.writer.serializer.MongoSerializationSchema
import org.apache.flink.connector.mongodb.sink.{
  MongoSink,
  MongoSinkBuilder
}
import org.apache.flink.streaming.api.scala.DataStream
import org.bson.BsonDocument

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

case class MongoDbSinkConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig)
    extends SinkConfig[ADT] {
  override def connector: FlinkConnectorName = MongoDb

  val uri: String        = config.getString(pfx("uri"))
  val database: String   = config.getString(pfx("database"))
  val collection: String = config.getString(pfx("collection"))

  val batchSize: Option[Int]        = config.getIntOpt(pfx("batch.size"))
  val batchIntervalMs: Option[Long] =
    config.getLongOpt(pfx("batch.interval.ms"))
  val maxRetries: Option[Int]       = config.getIntOpt(pfx("max.retries"))

  def getSerializationSchema[E <: ADT: TypeInformation](
      jsonFileEncoder: Encoder[E] = new JsonFileEncoder[E]())
      : MongoSerializationSchema[E] =
    new MongoSerializationSchema[E] {
      override def serialize(
          element: E,
          sinkContext: MongoSinkContext): WriteModel[BsonDocument] = {
        val baos         = new ByteArrayOutputStream()
        jsonFileEncoder.encode(element, baos)
        val bsonDocument =
          BsonDocument.parse(baos.toString(StandardCharsets.UTF_8))
        new InsertOneModel(
          bsonDocument
        ) // TODO: what about updates/upserts?
      }
    }

  def _getSink[E <: ADT: TypeInformation](
      serializationSchema: MongoSerializationSchema[E]): MongoSink[E] = {
    val builder = new MongoSinkBuilder[E]()
      .setUri(uri)
      .setDatabase(database)
      .setCollection(collection)
      .setSerializationSchema(serializationSchema)

    batchSize.foreach(builder.setBatchSize)
    batchIntervalMs.foreach(builder.setBatchIntervalMs)
    maxRetries.foreach(i => builder.setMaxRetries(i))
    deliveryGuarantee.foreach(g => builder.setDeliveryGuarantee(g))

    builder.build()
  }

  override def addSink[E <: ADT: TypeInformation](
      stream: DataStream[E]): Unit = {
    stream
      .sinkTo(_getSink[E](getSerializationSchema()))
      .name(label)
      .uid(uid)
      .setParallelism(parallelism)
    ()
  }

  override def addAvroSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](stream: DataStream[E]): Unit = {
    stream
      .sinkTo(
        _getSink[E](
          getSerializationSchema(
            new EmbeddedAvroJsonFileEncoder[E, A, ADT]()
          )
        )
      )
      .name(label)
      .uid(uid)
      .setParallelism(parallelism)
    ()
  }
}
