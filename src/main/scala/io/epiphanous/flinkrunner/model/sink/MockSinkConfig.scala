package io.epiphanous.flinkrunner.model.sink

import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkConnectorName, FlinkEvent}

case class MockSinkConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig,
    connector: FlinkConnectorName = FlinkConnectorName.MockSink)
    extends SinkConfig[ADT] {}
