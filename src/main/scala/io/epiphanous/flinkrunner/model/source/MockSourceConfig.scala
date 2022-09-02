package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkConnectorName, FlinkEvent}

/** A mock source for testing */
case class MockSourceConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig,
    connector: FlinkConnectorName = FlinkConnectorName.MockSource)
    extends SourceConfig[ADT] {}
