package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkConnectorName,
  FlinkEvent
}
import io.epiphanous.flinkrunner.serde.JsonKinesisDeserializationSchema
import io.epiphanous.flinkrunner.util.ConfigToProps.getFromEither
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.{Source, SourceSplit}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
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
  *     - `AT_SEQUENCE_NUMBER`: at the `starting.sequence` number
  *     - `AFTER_SEQUENCE_NUMBER`: just after the `starting.sequence`
  *       number
  *     - `AT_TIMESTAMP`: on or after the `starting.timestamp`
  *     - `TRIM_HORIZON`: the position of the earliest data in a shard
  *     - `LATEST`: the position after the most recent data in a shard
  *   - `starting.sequence`: a sequence number in a shard
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
    getFromEither(pfx(), Seq("aws.region"), config.getStringOpt).getOrElse(
      "us-east-1"
    )
  properties.setProperty("aws.region", awsRegion)

  val stream: String = Try(config.getString(pfx("stream"))).fold(
    t =>
      throw new RuntimeException(
        s"kinesis source $name configuration is missing a 'stream' property",
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
    ).getOrElse("TRIM_HORIZON").toUpperCase

    val validStartingPositions: Seq[String] = Seq(
      "LATEST",
      "TRIM_HORIZON",
      "AT_TIMESTAMP",
      "AT_SEQUENCE_NUMBER",
      "AFTER_SEQUENCE_NUMBER"
    )
    if (!validStartingPositions.contains(pos))
      throw new RuntimeException(
        s"Invalid starting position value <$pos>. Should be one of ${validStartingPositions.mkString(", ")}"
      )

    pos
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

  val startSeqNoOpt: Option[String] = getFromEither(
    pfx(),
    Seq(
      "starting.sequence",
      "starting.seq",
      "start.sequence",
      "start.seq"
    ),
    config.getStringOpt
  )

  properties.setProperty("flink.stream.initpos", startPos)
  (startPos, startTimestampOpt, startSeqNoOpt) match {
    case ("AT_TIMESTAMP", Some(ts), _) =>
      properties.setProperty("flink.stream.timestamp", ts)
    case ("AT_TIMESTAMP", None, _)     =>
      throw new RuntimeException(
        s"kinesis sink $name set starting.position to AT_TIMESTAMP but provided no starting.timestamp"
      )
    case (
          "AT_SEQUENCE_NUMBER" | "AFTER_SEQUENCE_NUMBER",
          _,
          Some(seqNo)
        ) =>
      properties.setProperty("flink.stream.sequence.number", seqNo)
    case (
          "AT_SEQUENCE_NUMBER" | "AFTER_SEQUENCE_NUMBER",
          _,
          None
        ) =>
      throw new RuntimeException(
        s"kinesis sink $name set starting.position to $startPos but provided no starting.sequence"
      )
    case _                             => // noop
  }

  val useEfo: Boolean =
    getFromEither(
      pfx(),
      Seq("use.efo", "efo.enabled"),
      config.getBooleanOpt
    ).getOrElse(
      properties
        .getProperty("flink.stream.recordpublisher", "EFO")
        .equalsIgnoreCase("EFO")
    )

  val efoConsumer: String = getFromEither(
    pfx(),
    Seq("efo.consumer", "flink.stream.efo.consumer"),
    config.getStringOpt
  ).getOrElse(s"${config.jobName}.$name")

  if (useEfo) {
    properties.setProperty("flink.stream.recordpublisher", "EFO")
    properties.setProperty(
      "flink.stream.efo.consumer",
      efoConsumer
    )
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
