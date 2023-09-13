package io.epiphanous.flinkrunner.model.sink

import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.util.AvroUtils.RichGenericRecord
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.sink2.SinkWriter
import org.apache.flink.connector.elasticsearch.sink
import org.apache.flink.connector.elasticsearch.sink.{
  Elasticsearch7SinkBuilder,
  ElasticsearchEmitter,
  FlushBackoffType
}
import org.apache.flink.streaming.api.scala.DataStream
import org.elasticsearch.client.Requests

import scala.collection.JavaConverters._

/** Elasticsearch sink config
  *
  * Configuration:
  *   - `index`: the name of the elasticsearch index to insert records into
  *   - `transports`: list of elasticsearch endpoints
  *   - `bulk.flush.backoff`:
  *     - `type`
  *     - `retries`
  *     - `delay`
  *   - `bulk.flush.max.actions`
  *   - `bulk.flush.max.size.mb`
  *   - `bulk.flush.interval.ms`
  *
  * @param name
  *   name of the sink
  * @param config
  *   flinkrunner configuration
  * @tparam ADT
  *   the flinkrunner algebraic data type
  */
case class ElasticsearchSinkConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig
) extends SinkConfig[ADT]
    with ElasticLikeConfigProps {

  override val connector: FlinkConnectorName =
    FlinkConnectorName.Elasticsearch

  def _addSink[E <: ADT: TypeInformation](
      dataStream: DataStream[E],
      emitter: ElasticsearchEmitter[E]): Unit = {
    val esb =
      new Elasticsearch7SinkBuilder[E]
        .setHosts(transports: _*)
        .setEmitter[E](emitter)
        .setBulkFlushBackoffStrategy(
          FlushBackoffType.valueOf(bulkFlushBackoffType),
          bulkFlushBackoffRetries,
          bulkFlushBackoffDelay
        )
    bulkFlushMaxActions.foreach(esb.setBulkFlushMaxActions)
    bulkFlushMaxSizeMb.foreach(esb.setBulkFlushMaxSizeMb)
    bulkFlushIntervalMs.foreach(esb.setBulkFlushInterval)
    dataStream
      .sinkTo(esb.build())
      .uid(label)
      .name(label)
      .setParallelism(parallelism)
    ()
  }

  override def addSink[E <: ADT: TypeInformation](
      dataStream: DataStream[E]): Unit =
    _addSink(dataStream, getEmitter[E])

  override def addAvroSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      dataStream: DataStream[E]): Unit =
    _addSink(dataStream, getAvroEmitter[E, A])

  def _getEmitter[E <: ADT](
      getData: E => AnyRef): ElasticsearchEmitter[E] =
    (element: E, _: SinkWriter.Context, indexer: sink.RequestIndexer) =>
      indexer.add(
        Requests.indexRequest
          .index(index)
          .source(
            Map("data" -> getData(element)).asJava
          )
      )

  def getEmitter[E <: ADT]: ElasticsearchEmitter[E] = _getEmitter { e =>
    val values = e.productIterator
    e.getClass.getDeclaredFields
      .map(_.getName -> values.next())
      .toMap
      .asJava
  }
  def getAvroEmitter[
      E <: ADT with EmbeddedAvroRecord[A],
      A <: GenericRecord]: ElasticsearchEmitter[E] = _getEmitter(
    _.$record.getDataAsMap.asJava
  )
}
