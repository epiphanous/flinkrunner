package io.epiphanous.flinkrunner.model.sink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkConnectorName.ElasticsearchSink
import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkConnectorName}
import org.apache.flink.api.connector.sink2.SinkWriter
import org.apache.flink.connector.elasticsearch.sink
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchEmitter
import org.elasticsearch.client.Requests

import java.util.Properties
import scala.collection.JavaConverters.mapAsJavaMap

case class ElasticsearchSinkConfig(
    config: FlinkConfig,
    connector: FlinkConnectorName = ElasticsearchSink,
    name: String,
    transports: List[String],
    index: String,
    properties: Properties)
    extends SinkConfig
    with LazyLogging {

  def getEmitter[E]: ElasticsearchEmitter[E] =
    new ElasticsearchEmitter[E] {
      override def emit(
          element: E,
          context: SinkWriter.Context,
          indexer: sink.RequestIndexer): Unit = {
        val data = Map("data" -> element.asInstanceOf[AnyRef])
        indexer.add(
          Requests.indexRequest.index(index).source(mapAsJavaMap(data))
        )
      }
    }
}
