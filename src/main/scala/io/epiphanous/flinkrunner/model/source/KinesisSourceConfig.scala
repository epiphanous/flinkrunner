package io.epiphanous.flinkrunner.model.source

import com.amazonaws.regions.Regions
import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkConnectorName,
  FlinkEvent
}
import io.epiphanous.flinkrunner.serde.JsonKinesisDeserializationSchema
import io.epiphanous.flinkrunner.util.ConfigToProps.getFromEither
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.{Source, SourceSplit}
import org.apache.flink.kinesis.shaded.org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_REGION
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants._
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema

import scala.util.Try

/** A source config for kinesis streams. For example, the following config
  * can be used to read from a topic in kafka that contains confluent avro
  * encoded messages.
  * {{{
  *   source my-kinesis-source {
  *     stream = "my-stream"
  *     starting.position = TRIM_HORIZON
  *     aws.region = us-east-1
  *   }
  * }}}
  *
  * Note: you must set up AWS access as described here in order to use
  * this.
  *
  * Configuration options:
  *   - `connector`: `kafka` (required only if it can't be inferred from
  *     the source name)
  *   - `stream`: the name of the kinesis stream
  *   - `starting.position`: the starting position of the stream; one of:
  *     - `TRIM_HORIZON`: the position of the earliest data in a shard
  *     - `LATEST`: the position after the most recent data in a shard
  *     - `AT_TIMESTAMP`: on or after the `starting.timestamp`
  *   - `starting.timestamp`: a timestamp as fractional epoch seconds
  *     (format: `yyyy-MM-dd'T'HH:mm:ss.SSSXXX`)
  *   - `use.efo`: if true, turn on enhanced fan-out to read the stream
  *     faster (defaults to true, may cost more money)
  *   - `efo.consumer`: name of the efo consumer (defaults to
  *     `jobName`.`sourceName`)
  *   - `aws.region`: AWS region of your kinesis endpoint
  *   - `config`: optional config to pass to kinesis client
  *
  * @param name
  *   name of the source
  * @param config
  *   flinkrunner config
  * @tparam ADT
  *   flinkrunner algebraic data type
  */
case class KinesisSourceConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig)
    extends SourceConfig[ADT] {

  override val connector: FlinkConnectorName = FlinkConnectorName.Kinesis

  val awsRegion: String =
    getFromEither(pfx(), Seq(AWS_REGION), config.getStringOpt).getOrElse(
      Regions.US_EAST_1.name()
    )
  properties.setProperty(AWS_REGION, awsRegion)

  val stream: String = Try(config.getString(pfx("stream"))).fold(
    t =>
      throw new RuntimeException(
        s"kinesis source $name is missing required 'stream' property",
        t
      ),
    s => s
  )

  val startPos: String = {
    val pos = getFromEither(
      pfx(),
      Seq(
        "starting.position",
        "starting.pos",
        "start.position",
        "start.pos",
        "flink.stream.initpos"
      ),
      config.getStringOpt
    ).getOrElse(InitialPosition.LATEST.name()).toUpperCase

    Try(InitialPosition.valueOf(pos)).fold(
      t =>
        throw new RuntimeException(
          s"Kinesis source $name has invalid `starting.position` <$pos>. Instead, use one of ${InitialPosition.values().map(_.name()).mkString(", ")}",
          t
        ),
      sit => sit.name()
    )
  }

  val startTimestampOpt: Option[String] = getFromEither(
    pfx(),
    Seq(
      "starting.timestamp",
      "starting.ts",
      "start.timestamp",
      "start.ts"
    ),
    config.getStringOpt
  )

  properties.setProperty(STREAM_INITIAL_POSITION, startPos)
  if (
    startPos.equalsIgnoreCase(
      InitialPosition.AT_TIMESTAMP.name()
    )
  ) {
    startTimestampOpt.fold(
      throw new RuntimeException(
        s"kinesis sink $name set starting.position to AT_TIMESTAMP but provided no starting.timestamp"
      )
    )(ts => properties.setProperty(STREAM_INITIAL_TIMESTAMP, ts))
  }

  val useEfo: Boolean =
    getFromEither(
      pfx(),
      Seq("use.efo", "efo.enabled"),
      config.getBooleanOpt
    ).getOrElse(
      properties
        .getProperty(RECORD_PUBLISHER_TYPE, "EFO")
        .equalsIgnoreCase("EFO")
    )

  val efoConsumer: String = getFromEither(
    pfx(),
    Seq("efo.consumer", "flink.stream.efo.consumer", EFO_CONSUMER_NAME),
    config.getStringOpt
  ).getOrElse(s"${config.jobName}.$name")

  val efoRegistrationType : String = getFromEither(
    pfx(),
    Seq("efo.registration.type", "flink.stream.efo.registration.type", EFO_REGISTRATION_TYPE),
    config.getStringOpt
  ).getOrElse(EFORegistrationType.LAZY.name())

  if (useEfo) {
    properties.setProperty(RECORD_PUBLISHER_TYPE, "EFO")
    properties.setProperty(EFO_CONSUMER_NAME, efoConsumer)
    properties.setProperty(EFO_REGISTRATION_TYPE, efoRegistrationType)
  }

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
