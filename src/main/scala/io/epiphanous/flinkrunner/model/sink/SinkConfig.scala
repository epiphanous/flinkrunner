package io.epiphanous.flinkrunner.model.sink

import io.epiphanous.flinkrunner.model.FlinkConnectorName._
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.util.AvroUtils.rowTypeOf
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.types.Row

import scala.util.Try

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
trait SinkConfig[ADT <: FlinkEvent] extends SourceOrSinkConfig {

  override val _sourceOrSink = "sinks"

  def addSink[E <: ADT: TypeInformation](stream: DataStream[E]): Unit

  def addAvroSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation
  ](
      stream: DataStream[E]
  ): Unit

  def addRowSink[E <: ADT with EmbeddedRowType: TypeInformation](
      stream: DataStream[E]): Unit =
    _addRowSinkStream[E](stream, configuredRowType)

  def _addRowSink(rows: DataStream[Row], rowType: RowType): Unit = ???

  def _addRowSinkStream[E <: ADT with EmbeddedRowType](
      dataStream: DataStream[E],
      maybeRowType: Try[RowType]): Unit = {
    maybeRowType.fold(
      t =>
        throw new RuntimeException(
          s"can't determine row.type for iceberg sink $name",
          t
        ),
      rowType =>
        _addRowSink(
          dataStream
            .map((e: E) => e.toRow)
            .name(s"row:${dataStream.name}")
            .uid(s"row:${dataStream.name}")
            .setParallelism(dataStream.parallelism),
          rowType
        )
    )
  }

  def addAvroRowSink[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](stream: DataStream[E]): Unit =
    _addRowSinkStream(
      stream,
      configuredRowType
        .orElse(rowTypeOf(implicitly[TypeInformation[A]].getTypeClass))
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
