package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.model.sink.SinkConfig
import io.epiphanous.flinkrunner.util.ConfigToProps.{
  getFromEither,
  RichConfigObject
}
import org.apache.flink.connector.aws.config.AWSConfigConstants

import java.util.Properties

case class KinesisProperties(
    stream: String,
    clientProperties: Properties,
    failOnError: Boolean,
    maxBatchSizeInNumber: Int,
    maxBatchSizeInBytes: Long,
    maxBufferedRequests: Int,
    maxBufferTime: Long,
    maxInFlightRequests: Int,
    maxRecordSizeInBytes: Option[Long])

object KinesisProperties {
  def fromSinkConfig[SC <: SinkConfig[_]](
      sinkConfig: SC): KinesisProperties = {
    val config = sinkConfig.config
    val pfx    = sinkConfig.pfx()

    val awsRegion: Option[String] = getFromEither(
      pfx,
      Seq("aws.region", "region", AWSConfigConstants.AWS_REGION),
      config.getStringOpt
    )

    val awsEndpoint: Option[String] = getFromEither(
      pfx,
      Seq("aws.endpoint", "endpoint", AWSConfigConstants.AWS_ENDPOINT),
      config.getStringOpt
    )

    val clientProperties: Properties =
      getFromEither(
        pfx,
        Seq("client"),
        config.getObjectOption
      ).asProperties

    clientProperties.putIfAbsent(
      AWSConfigConstants.AWS_REGION,
      awsRegion.getOrElse("us-east-1")
    )

    awsEndpoint.foreach(endpoint =>
      clientProperties.putIfAbsent(
        AWSConfigConstants.AWS_ENDPOINT,
        endpoint
      )
    )

    val stream: String = getFromEither(
      pfx,
      Seq(
        "stream",
        "stream.name",
        "delivery.stream",
        "delivery.stream.name"
      ),
      config.getStringOpt
    ).getOrElse(
      throw new RuntimeException(
        s"kinesis stream name required but missing in sink ${sinkConfig.name} of job ${config.jobName}"
      )
    )

    val failOnError: Boolean =
      config.getBooleanOpt("fail.on.error").getOrElse(false)

    val maxInFlightRequests: Int =
      config.getIntOpt("max.in.flight.requests").getOrElse(50)

    val maxBufferedRequests: Int =
      config.getIntOpt("max.buffer.requests").getOrElse(10000)

    val maxBatchSizeInNumber: Int =
      config.getIntOpt("max.batch.size.number").getOrElse(500)

    val maxBatchSizeInBytes: Long =
      config.getLongOpt("max.batch.size.bytes").getOrElse(4 * 1024 * 1024)

    val maxBufferTime: Long = config
      .getDurationOpt("max.buffer.time")
      .map(_.toMillis)
      .getOrElse(5000)

    val maxRecordSize: Option[Long] =
      config.getLongOpt("max.record.size")

    KinesisProperties(
      stream,
      clientProperties,
      failOnError,
      maxBatchSizeInNumber,
      maxBatchSizeInBytes,
      maxBufferedRequests,
      maxBufferTime,
      maxInFlightRequests,
      maxRecordSize
    )
  }

}
