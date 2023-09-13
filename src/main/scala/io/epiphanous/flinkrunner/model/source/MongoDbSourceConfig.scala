package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.model.FlinkConnectorName.MongoDb
import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkConnectorName,
  FlinkEvent
}

case class MongoDbSourceConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig)
    extends SourceConfig[ADT] {
  override def connector: FlinkConnectorName = MongoDb

}
