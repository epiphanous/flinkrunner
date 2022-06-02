package io.epiphanous.flinkrunner.model.sink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkConnectorName.CassandraSink
import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkConnectorName}

import java.util.Properties

case class CassandraSinkConfig(
    config: FlinkConfig,
    connector: FlinkConnectorName = CassandraSink,
    name: String,
    host: String,
    query: String,
    properties: Properties)
    extends SinkConfig
    with LazyLogging
