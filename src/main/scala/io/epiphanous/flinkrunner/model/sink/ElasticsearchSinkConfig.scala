package io.epiphanous.flinkrunner.model.sink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.{EmbeddedAvroRecord, FlinkConfig, FlinkConnectorName, FlinkEvent}
import io.epiphanous.flinkrunner.util.AvroUtils.RichGenericRecord
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.sink2.SinkWriter
import org.apache.flink.connector.elasticsearch.sink
import org.apache.flink.connector.elasticsearch.sink.{Elasticsearch7SinkBuilder, ElasticsearchEmitter, FlushBackoffType}
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import java.net.URL
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
    with LazyLogging {

  override val connector: FlinkConnectorName =
    FlinkConnectorName.Elasticsearch

  val index: String              = config.getString(pfx("index"))
  val transports: List[HttpHost] =
    config.getStringList(pfx("transports")).map { s =>
      val url      = new URL(if (s.startsWith("http")) s else s"http://$s")
      val hostname = url.getHost
      val port     = if (url.getPort < 0) 9200 else url.getPort
      new HttpHost(hostname, port, url.getProtocol)
    }

  val bulkFlushBackoffType: FlushBackoffType = FlushBackoffType
    .valueOf(properties.getProperty("bulk.flush.backoff.type", "NONE"))

  val bulkFlushBackoffRetries: Int =
    properties.getProperty("bulk.flush.backoff.retries", "5").toInt

  val bulkFlushBackoffDelay: Long =
    properties.getProperty("bulk.flush.backoff.delay", "1000").toLong

  val bulkFlushMaxActions: Option[Int]  =
    Option(properties.getProperty("bulk.flush.max.actions")).map(_.toInt)
  val bulkFlushMaxSizeMb: Option[Int]   =
    Option(properties.getProperty("bulk.flush.max.size.mb")).map(_.toInt)
  val bulkFlushIntervalMs: Option[Long] =
    Option(properties.getProperty("bulk.flush.interval.ms")).map(_.toLong)

  def _getSink[E <: ADT: TypeInformation](
      dataStream: DataStream[E],
      emitter: ElasticsearchEmitter[E]): DataStreamSink[E] = {
    val esb =
      new Elasticsearch7SinkBuilder[E]
        .setHosts(transports: _*)
        .setEmitter[E](emitter)
        .setBulkFlushBackoffStrategy(
          bulkFlushBackoffType,
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
  }

  override def getSink[E <: ADT: TypeInformation](
      dataStream: DataStream[E]): DataStreamSink[E] =
    _getSink(dataStream, getEmitter[E])

  override def getAvroSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      dataStream: DataStream[E]): DataStreamSink[E] =
    _getSink(dataStream, getAvroEmitter[E, A])

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
