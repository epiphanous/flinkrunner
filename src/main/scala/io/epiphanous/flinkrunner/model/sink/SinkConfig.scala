package io.epiphanous.flinkrunner.model.sink

import com.google.common.collect.Maps
import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkConnectorName._
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.util.ConfigToProps
import org.apache.flink.connector.base.DeliveryGuarantee

import java.util
import java.util.Properties

trait SinkConfig extends LazyLogging {
  def config: FlinkConfig

  def connector: FlinkConnectorName

  def name: String

  def label: String = s"$connector/$name"

  def properties: Properties

  def propertiesMap: util.HashMap[String, String] =
    Maps.newHashMap(Maps.fromProperties(properties))
}

object SinkConfig {
  def apply(name: String, config: FlinkConfig): SinkConfig = {
    val p = s"sinks.$name"

    val connector = FlinkConnectorName
      .fromSinkName(
        name,
        config.jobName,
        config.getStringOpt(s"$p.connector")
      )

    connector match {
      case Kafka             =>
        val props = ConfigToProps.normalizeProps(
          config,
          p,
          List("bootstrap.servers")
        )
        KafkaSinkConfig(
          config,
          connector,
          name,
          config.getString(s"$p.topic"),
          config.getBoolean(s"$p.isKeyed"),
          props.getProperty("bootstrap.servers"),
          config
            .getStringOpt(s"$p.delivery.guarantee")
            .map(s => s.toLowerCase.replaceAll("[^a-z]+", "-")) match {
            case Some("at-least-once") =>
              DeliveryGuarantee.AT_LEAST_ONCE
            case Some("none")          =>
              DeliveryGuarantee.NONE
            case _                     => DeliveryGuarantee.EXACTLY_ONCE
          },
          props
        )
      case Kinesis           =>
        KinesisSinkConfig(
          config,
          connector,
          name,
          config.getString(s"$p.stream"),
          config.getProperties(s"$p.config")
        )
      case File              =>
        val rowFormat = StreamFormatName.withNameInsensitiveOption(
          config.getString(s"$p.format")
        )
        FileSinkConfig(
          config,
          connector,
          name,
          config.getString(s"$p.path"),
          rowFormat.isEmpty,
          rowFormat,
          config.getProperties(s"$p.config")
        )
      case Socket            =>
        SocketSinkConfig(
          config,
          connector,
          name,
          config.getString(s"$p.host"),
          config.getInt(s"$p.port"),
          StreamFormatName.withNameInsensitive(
            config.getString(s"$p.format")
          ),
          config.getIntOpt(s"$p.max.retries"),
          config.getBooleanOpt(s"$p.auto.flush"),
          config.getProperties(s"$p.config")
        )
      case Jdbc              =>
        JdbcSinkConfig(
          config,
          connector,
          name,
          config.getString(s"$p.url"),
          config.getString(s"$p.query"),
          config
            .getObjectList(s"$p.params")
            .map(_.toConfig)
            .map(c =>
              JdbcStatementParam(
                c.getString("name"),
                c.getString("jdbc.type")
              )
            ),
          config.getProperties(s"$p.config")
        )
      case CassandraSink     =>
        CassandraSinkConfig(
          config,
          connector,
          name,
          config.getString(s"$p.host"),
          config.getString(s"$p.query"),
          config.getProperties(s"$p.config")
        )
      case ElasticsearchSink =>
        ElasticsearchSinkConfig(
          config,
          connector,
          name,
          config.getStringList(s"$p.transports"),
          config.getString(s"$p.index"),
          config.getProperties(s"$p.config")
        )
      case RabbitMQ          =>
        val c   = config.getProperties(s"$p.config")
        val uri = config.getString(s"$p.uri")
        RabbitMQSinkConfig(
          config,
          connector,
          name,
          uri,
          config.getBoolean(s"$p.use.correlation.id"),
          RabbitMQConnectionInfo(uri, c),
          Option(c.getProperty("queue")),
          c
        )
      case _                 =>
        throw new RuntimeException(
          s"Don't know how to configure ${connector.entryName} sink connector $name (job ${config.jobName}"
        )
    }
  }
}
