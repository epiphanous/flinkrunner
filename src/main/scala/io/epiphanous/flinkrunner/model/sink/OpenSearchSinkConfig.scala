package io.epiphanous.flinkrunner.model.sink

import io.epiphanous.flinkrunner.model.FlinkConnectorName.OpenSearch
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.util.AvroUtils._
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.opensearch.sink._
import org.apache.flink.streaming.api.scala.DataStream
import org.opensearch.client.Requests

import scala.collection.JavaConverters._

case class OpenSearchSinkConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig)
    extends SinkConfig[ADT]
    with ElasticLikeConfigProps {
  override def connector: FlinkConnectorName = OpenSearch

  val allowInsecure: Option[Boolean]               =
    config.getBooleanOpt(pfx("allow.insecure"))
  val connectionUsername: Option[String]           =
    config.getStringOpt(pfx("connection.username"))
  val connectionPassword: Option[String]           =
    config.getStringOpt(pfx("connection.password"))
  val connectionPathPrefix: Option[String]         =
    config.getStringOpt(pfx("connection.path.prefix"))
  val connectionRequestTimeout: Option[Int]        =
    config.getIntOpt(pfx("connection.request.timeout"))
  val connectionTimeout: Option[Int]               =
    config.getIntOpt(pfx("connection.timeout"))
  val socketTimeout: Option[Int]                   =
    config.getIntOpt(pfx("socket.timeout"))

  def _getSink[E <: ADT: TypeInformation](
      emitter: OpensearchEmitter[E]): OpensearchSink[E] = {
    val builder = new OpensearchSinkBuilder[E]
      .setHosts(transports: _*)
      .setEmitter[E](emitter)

    builder
      .setBulkFlushBackoffStrategy(
        FlushBackoffType.valueOf(bulkFlushBackoffType),
        bulkFlushBackoffRetries,
        bulkFlushBackoffDelay
      )

    allowInsecure.foreach(builder.setAllowInsecure)

    bulkFlushIntervalMs.foreach(builder.setBulkFlushInterval)
    bulkFlushMaxSizeMb.foreach(builder.setBulkFlushMaxSizeMb)
    bulkFlushMaxActions.foreach(builder.setBulkFlushMaxActions)

    connectionUsername.foreach(builder.setConnectionUsername)
    connectionPassword.foreach(builder.setConnectionPassword)
    connectionPathPrefix.foreach(builder.setConnectionPathPrefix)
    connectionRequestTimeout.foreach(builder.setConnectionRequestTimeout)
    connectionTimeout.foreach(builder.setConnectionTimeout)

    deliveryGuarantee.foreach(g => builder.setDeliveryGuarantee(g))

    socketTimeout.foreach(builder.setSocketTimeout)

    builder.build()
  }

  override def addSink[E <: ADT: TypeInformation](
      stream: DataStream[E]): Unit = {
    stream
      .sinkTo(_getSink[E](getEmitter[E]))
      .uid(label)
      .name(label)
      .setParallelism(parallelism)
    ()
  }

  override def addAvroSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](stream: DataStream[E]): Unit = {
    stream
      .sinkTo(_getSink[E](getAvroEmitter[E, A]))
      .uid(label)
      .name(label)
      .setParallelism(parallelism)
    ()
  }

  def _getEmitter[E <: ADT](getData: E => AnyRef = _.asInstanceOf[AnyRef])
      : OpensearchEmitter[E] =
    (element: E, _, indexer: RequestIndexer) =>
      indexer.add(
        Requests.indexRequest
          .index(index)
          .source(
            Map("data" -> getData(element)).asJava
          )
      )

  def getEmitter[E <: ADT]: OpensearchEmitter[E] = _getEmitter()

  def getAvroEmitter[
      E <: ADT with EmbeddedAvroRecord[A],
      A <: GenericRecord]: OpensearchEmitter[E] = _getEmitter(
    _.$record.getDataAsMap
  )

}
