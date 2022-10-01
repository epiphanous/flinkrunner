package io.epiphanous.flinkrunner.model.sink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model.FlinkConnectorName._
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.util.StreamUtils._
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.util
import java.util.Properties

/** A flinkrunner sink configuration trait. All sink configs have a few
  * common configuration options.
  *
  * Common Configuration Options:
  *
  *   - `name`: the sink name
  *   - `connector`: one of
  *     - [[FlinkConnectorName.CassandraSink]]
  *     - [[FlinkConnectorName.ElasticsearchSink]]
  *     - [[FlinkConnectorName.File]]
  *     - [[FlinkConnectorName.Jdbc]]
  *     - [[FlinkConnectorName.Kafka]]
  *     - [[FlinkConnectorName.Kinesis]]
  *     - [[FlinkConnectorName.RabbitMQ]]
  *     - [[FlinkConnectorName.Socket]]
  * @tparam ADT
  *   the flinkrunner algebraic data type
  */
trait SinkConfig[ADT <: FlinkEvent] extends LazyLogging {
  def name: String
  def runner: FlinkRunner[ADT]
  def connector: FlinkConnectorName

  val config: FlinkConfig = runner.config

  def pfx(path: String = ""): String = Seq(
    Some("sinks"),
    Some(name),
    if (path.isEmpty) None else Some(path)
  ).flatten.mkString(".")

  val properties: Properties = config.getProperties(pfx("config"))

  lazy val propertiesMap: util.HashMap[String, String] =
    properties.asJavaMap

  lazy val label: String = s"${connector.entryName.toLowerCase}/$name"

}

object SinkConfig {
  def apply[ADT <: FlinkEvent: TypeInformation](
      name: String,
      runner: FlinkRunner[ADT]): SinkConfig[ADT] = {

    FlinkConnectorName
      .fromSinkName(
        name,
        runner.config.jobName,
        runner.config.getStringOpt(s"sinks.$name.connector")
      ) match {
      case Kafka             => KafkaSinkConfig(name, runner)
      case Kinesis           => KinesisSinkConfig(name, runner)
      case File              => FileSinkConfig(name, runner)
      case Socket            => SocketSinkConfig(name, runner)
      case Jdbc              => JdbcSinkConfig(name, runner)
      case CassandraSink     =>
        CassandraSinkConfig(name, runner)
      case ElasticsearchSink =>
        ElasticsearchSinkConfig(name, runner)
      case RabbitMQ          => RabbitMQSinkConfig(name, runner)
      case connector         =>
        throw new RuntimeException(
          s"Don't know how to configure ${connector.entryName} sink connector $name (job ${runner.config.jobName}"
        )
    }
  }
}
