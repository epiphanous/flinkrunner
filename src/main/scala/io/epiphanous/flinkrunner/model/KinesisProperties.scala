package io.epiphanous.flinkrunner.model

import com.amazonaws.regions.Regions
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

  final val DEFAULT_REGION                   = Regions.US_EAST_1.getName
  final val DEFAULT_FAIL_ON_ERROR            = false
  final val DEFAULT_MAX_BATCH_SIZE_IN_NUMBER = 500
  final val DEFAULT_MAX_BATCH_SIZE_IN_BYTES  = 4 * 1024 * 1024
  final val DEFAULT_MAX_BUFFERED_REQUESTS    = 10000
  final val DEFAULT_MAX_BUFFER_TIME          = 5000
  final val DEFAULT_MAX_IN_FLIGHT_REQUESTS   = 50

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
      awsRegion.getOrElse(DEFAULT_REGION)
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
        s"kinesis stream name required but missing in sink <${sinkConfig.name}> of job <${config.jobName}>"
      )
    )

    val failOnError: Boolean = getFromEither(
      pfx,
      Seq("failOnError", "fail.on.error"),
      config.getBooleanOpt
    ).getOrElse(DEFAULT_FAIL_ON_ERROR)

    val maxInFlightRequests: Int = getFromEither(
      pfx,
      Seq("maxInFlightRequests", "max.in.flight.requests"),
      config.getIntOpt
    ).getOrElse(DEFAULT_MAX_IN_FLIGHT_REQUESTS)

    val maxBufferedRequests: Int =
      getFromEither(
        pfx,
        Seq("maxBufferedRequests", "max.buffered.requests"),
        config.getIntOpt
      ).getOrElse(DEFAULT_MAX_BUFFERED_REQUESTS)

    val maxBatchSizeInNumber: Int =
      getFromEither(
        pfx,
        Seq(
          "maxBatchSizeInNumber",
          "max.batch.size.in.number",
          "max.batch.size.number"
        ),
        config.getIntOpt
      ).getOrElse(DEFAULT_MAX_BATCH_SIZE_IN_NUMBER)

    val maxBatchSizeInBytes: Long =
      getFromEither(
        pfx,
        Seq(
          "maxBatchSizeInBytes",
          "max.batch.size.in.bytes",
          "max.batch.size.bytes"
        ),
        config.getLongOpt
      ).getOrElse(DEFAULT_MAX_BATCH_SIZE_IN_BYTES)

    val maxBufferTime: Long = getFromEither(
      pfx,
      Seq("maxBufferTime", "max.buffer.time"),
      config.getDurationOpt
    )
      .map(_.toMillis)
      .getOrElse(DEFAULT_MAX_BUFFER_TIME)

    val maxRecordSizeInBytes: Option[Long] = getFromEither(
      pfx,
      Seq(
        "maxRecordSizeInBytes",
        "maxRecordSize",
        "max.record.size",
        "max.record.size.in.bytes"
      ),
      config.getLongOpt
    )

    KinesisProperties(
      stream,
      clientProperties,
      failOnError,
      maxBatchSizeInNumber,
      maxBatchSizeInBytes,
      maxBufferedRequests,
      maxBufferTime,
      maxInFlightRequests,
      maxRecordSizeInBytes
    )
  }

}
