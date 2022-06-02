package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.model.FlinkConnectorName.Kinesis
import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkConnectorName,
  FlinkEvent
}
import io.epiphanous.flinkrunner.serde.JsonKinesisDeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema

import java.util.Properties

case class KinesisSourceConfig(
    config: FlinkConfig,
    connector: FlinkConnectorName = Kinesis,
    name: String,
    stream: String,
    watermarkStrategy: String,
    maxAllowedLateness: Long,
    properties: Properties)
    extends SourceConfig {

  /**
   * Returns a deserialization schema for kinesis. This implementation
   * assumes JSON formatted event records.
   * @tparam E
   *   the event type
   * @return
   *   KinesisDeserializationSchema[E]
   */
  def getDeserializationSchema[E <: FlinkEvent: TypeInformation]
      : KinesisDeserializationSchema[E] =
    new JsonKinesisDeserializationSchema[E](this)

}
