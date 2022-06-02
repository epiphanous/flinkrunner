package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.model.FlinkConnectorName.Collection
import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkConnectorName}

import java.util.Properties

case class CollectionSourceConfig(
    config: FlinkConfig,
    connector: FlinkConnectorName = Collection,
    name: String,
    topic: String,
    watermarkStrategy: String,
    maxAllowedLateness: Long,
    properties: Properties)
    extends SourceConfig
