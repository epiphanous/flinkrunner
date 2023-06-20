package io.epiphanous.flinkrunner.model.sink

import io.epiphanous.flinkrunner.model.FlinkConnectorName._
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.util.RowUtils.rowTypeOf
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.types.Row

import scala.reflect.runtime.{universe => ru}

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
  *     - [[FlinkConnectorName.Firehose]]
  *     - [[FlinkConnectorName.Jdbc]]
  *     - [[FlinkConnectorName.Kafka]]
  *     - [[FlinkConnectorName.Kinesis]]
  *     - [[FlinkConnectorName.RabbitMQ]]
  *     - [[FlinkConnectorName.Socket]]
  *
  * @tparam ADT
  *   the flinkrunner algebraic data type
  */
trait SinkConfig[ADT <: FlinkEvent] extends SourceOrSinkConfig[ADT] {

  override def _sourceOrSink = "sink"

  def addSink[E <: ADT: TypeInformation](stream: DataStream[E]): Unit

  def addAvroSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation
  ](
      stream: DataStream[E]
  ): Unit

  def addRowSink[
      E <: ADT with EmbeddedRowType: TypeInformation: ru.TypeTag](
      stream: DataStream[E]): Unit =
    _addRowSink(
      stream
        .map((e: E) => e.toRow)
        .name(s"row:${stream.name}")
        .uid(s"row:${stream.name}")
        .setParallelism(stream.parallelism),
      rowTypeOf[E]
    )

  def _addRowSink(rows: DataStream[Row], rowType: RowType): Unit = ???

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
      case Firehose      => FirehoseSinkConfig(name, config)
      case File          => FileSinkConfig(name, config)
      case Socket        => SocketSinkConfig(name, config)
      case Jdbc          => JdbcSinkConfig(name, config)
      case Cassandra     =>
        CassandraSinkConfig(name, config)
      case Elasticsearch =>
        ElasticsearchSinkConfig(name, config)
      case RabbitMQ      => RabbitMQSinkConfig(name, config)
      case Iceberg       => IcebergSinkConfig(name, config)
      case Print         => PrintSinkConfig(name, config)
      case connector     =>
        throw new RuntimeException(
          s"Don't know how to configure ${connector.entryName} sink connector <$name> (in job <${config.jobName}>)"
        )
    }
  }
}
