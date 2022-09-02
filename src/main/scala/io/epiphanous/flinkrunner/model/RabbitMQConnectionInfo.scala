package io.epiphanous.flinkrunner.model

import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig

import java.util.Properties

case class RabbitMQConnectionInfo(
    uri: String,
    networkRecoveryInterval: Option[Int] = None,
    automaticRecovery: Option[Boolean] = None,
    topologyRecovery: Option[Boolean] = None,
    connectionTimeout: Option[Int] = None,
    requestedChannelMax: Option[Int] = None,
    requestedFrameMax: Option[Int] = None,
    requestedHeartbeat: Option[Int] = None,
    prefetchCount: Option[Int] = None,
    deliveryTimeout: Option[Long] = None) {
  def rmqConfig: RMQConnectionConfig = {
    var cb =
      new RMQConnectionConfig.Builder().setUri(uri)
    cb = automaticRecovery
      .map(cb.setAutomaticRecovery)
      .getOrElse(cb)
    cb = connectionTimeout
      .map(cb.setConnectionTimeout)
      .getOrElse(cb)
    cb = deliveryTimeout
      .map(cb.setDeliveryTimeout)
      .getOrElse(cb)
    cb = networkRecoveryInterval
      .map(cb.setNetworkRecoveryInterval)
      .getOrElse(cb)
    cb = prefetchCount
      .map(cb.setPrefetchCount)
      .getOrElse(cb)
    cb = requestedChannelMax
      .map(cb.setRequestedChannelMax)
      .getOrElse(cb)
    cb = requestedFrameMax
      .map(cb.setRequestedFrameMax)
      .getOrElse(cb)
    cb = requestedHeartbeat
      .map(cb.setRequestedHeartbeat)
      .getOrElse(cb)
    cb = topologyRecovery
      .map(cb.setTopologyRecoveryEnabled)
      .getOrElse(cb)
    cb.build()
  }
}
object RabbitMQConnectionInfo             {
  def apply(uri: String, c: Properties): RabbitMQConnectionInfo = {
    RabbitMQConnectionInfo(
      uri,
      Option(c.getProperty("network.recovery.interval"))
        .map(_.toInt),
      Option(c.getProperty("automatic.recovery")).map(_.toBoolean),
      Option(c.getProperty("topology.recovery")).map(_.toBoolean),
      Option(c.getProperty("connection.timeout")).map(_.toInt),
      Option(c.getProperty("requested.channel.max")).map(_.toInt),
      Option(c.getProperty("requested.frame.max")).map(_.toInt),
      Option(c.getProperty("requested.heartbeat")).map(_.toInt),
      Option(c.getProperty("prefetch.count")).map(_.toInt),
      Option(c.getProperty("delivery.timeout")).map(_.toLong)
    )
  }
}
