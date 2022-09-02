package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkConnectorName, FlinkEvent}
import io.epiphanous.flinkrunner.serde.JsonKinesisDeserializationSchema
import io.epiphanous.flinkrunner.util.ConfigToProps
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.{Source, SourceSplit}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema

import java.util.Properties

case class KinesisSourceConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig,
    connector: FlinkConnectorName = FlinkConnectorName.Kinesis)
    extends SourceConfig[ADT] {

  override val properties: Properties = ConfigToProps.normalizeProps(
    config,
    pfx(),
    List("aws.region", "flink.stream.initpos")
  )

  val stream: String  = config.getString(pfx("stream"))
  val initPos: String = properties.getProperty("flink.stream.initpos")

  properties.setProperty("flink.stream.recordpublisher", "EFO")
  properties.setProperty(
    "flink.stream.efo.consumer",
    s"${config.jobName}.$name"
  )

  /** Returns a deserialization schema for kinesis. This implementation
    * assumes JSON formatted event records.
    * @tparam E
    *   the event type
    * @return
    *   KinesisDeserializationSchema[E]
    */
  def getDeserializationSchema[E <: ADT: TypeInformation]
      : KinesisDeserializationSchema[E] =
    new JsonKinesisDeserializationSchema[E, ADT](this)

  override def getSource[E <: ADT: TypeInformation]
      : Either[SourceFunction[E], Source[E, _ <: SourceSplit, _]] =
    Left(
      new FlinkKinesisConsumer[E](
        stream,
        getDeserializationSchema,
        properties
      )
    )

}
