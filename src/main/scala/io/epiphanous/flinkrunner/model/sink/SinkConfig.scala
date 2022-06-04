package io.epiphanous.flinkrunner.model.sink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkConnectorName._
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.util.StreamUtils._
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.util
import java.util.Properties

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

  val label: String = s"${connector.entryName.toLowerCase}/$name"

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
      case Kafka             => KafkaSinkConfig(name, config, Kafka)
      case Kinesis           => KinesisSinkConfig(name, config, Kinesis)
      case File              => FileSinkConfig(name, config, File)
      case Socket            => SocketSinkConfig(name, config, Socket)
      case Jdbc              => JdbcSinkConfig(name, config, Jdbc)
      case CassandraSink     =>
        CassandraSinkConfig(name, config, CassandraSink)
      case ElasticsearchSink =>
        ElasticsearchSinkConfig(name, config, ElasticsearchSink)
      case RabbitMQ          => RabbitMQSinkConfig(name, config, RabbitMQ)
      case connector         =>
        throw new RuntimeException(
          s"Don't know how to configure ${connector.entryName} sink connector $name (job ${config.jobName}"
        )
    }
  }
}
