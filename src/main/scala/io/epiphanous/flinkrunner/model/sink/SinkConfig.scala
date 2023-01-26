package io.epiphanous.flinkrunner.model.sink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkConnectorName._
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.util.StreamUtils._
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.table.types.logical.utils.LogicalTypeParser
import org.apache.flink.types.Row

import java.util
import java.util.Properties

/** A flinkrunner sink configuration trait. All sink configs have a few
  * common configuration options.
  *
  * Common Configuration Options:
  *
  *   - `name`: the sink name
  *   - `connector`: one of
  *     - [[FlinkConnectorName.Cassandra]]
  *     - [[FlinkConnectorName.Elasticsearch]]
  *     - [[FlinkConnectorName.File]]
  *     - [[FlinkConnectorName.Jdbc]]
  *     - [[FlinkConnectorName.Kafka]]
  *     - [[FlinkConnectorName.Kinesis]]
  *     - [[FlinkConnectorName.RabbitMQ]]
  *     - [[FlinkConnectorName.Socket]]
  *
  * @tparam ADT
  *   the flinkrunner algebraic data type
  */
trait SinkConfig[ADT <: FlinkEvent] extends LazyLogging {
  def name: String
  def config: FlinkConfig
  def connector: FlinkConnectorName

  def pfx(path: String = ""): String = Seq(
    Some("sinks"),
    Some(name),
    if (path.isEmpty) None else Some(path)
  ).flatten.mkString(".")

  val properties: Properties = config.getProperties(pfx("config"))

  lazy val propertiesMap: util.HashMap[String, String] =
    properties.asJavaMap

  lazy val label: String = s"${connector.entryName.toLowerCase}/$name"

  lazy val configuredRowType: Option[RowType] = config
    .getStringOpt(pfx("row"))
    .map(t =>
      RowType.of(
        false,
        LogicalTypeParser
          .parse(t, Thread.currentThread.getContextClassLoader)
      )
    )

  def addSink[E <: ADT: TypeInformation](stream: DataStream[E]): Unit

  def addAvroSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation
  ](
      stream: DataStream[E]
  ): Unit

  def addRowSink(stream: DataStream[Row], rowType: RowType): Unit =
    throw new RuntimeException(
      s"addRowSink is not implemented for ${connector.entryName} sink $name"
    )

}

object SinkConfig {
  def apply[ADT <: FlinkEvent: TypeInformation](
      name: String,
      config: FlinkConfig): SinkConfig[ADT] = {

    FlinkConnectorName
      .fromSinkName(
        name,
        config.jobName,
        config.getStringOpt(s"sinks.$name.connector")
      ) match {
      case Kafka         => KafkaSinkConfig(name, config)
      case Kinesis       => KinesisSinkConfig(name, config)
      case File          => FileSinkConfig(name, config)
      case Socket        => SocketSinkConfig(name, config)
      case Jdbc          => JdbcSinkConfig(name, config)
      case Cassandra     =>
        CassandraSinkConfig(name, config)
      case Elasticsearch =>
        ElasticsearchSinkConfig(name, config)
      case RabbitMQ      => RabbitMQSinkConfig(name, config)
      case connector     =>
        throw new RuntimeException(
          s"Don't know how to configure ${connector.entryName} sink connector $name (job ${config.jobName}"
        )
    }
  }
}
