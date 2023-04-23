package io.epiphanous.flinkrunner.model.sink

import com.amazonaws.regions.Regions
import io.epiphanous.flinkrunner.model.{
  KinesisProperties,
  MySimpleADT,
  SimpleA
}
import io.epiphanous.flinkrunner.{FlinkRunner, PropSpec}
import org.apache.flink.api.scala.createTypeInformation

class FirehoseSinkConfigSpec extends PropSpec {

  def testConfig(config: String): FlinkRunner[MySimpleADT] =
    getIdentityStreamJobRunner[SimpleA, MySimpleADT](
      s"""
         |jobs {
         |  testJob {
         |    sinks {
         |      firehose-sink {
         |      $config
         |      }
         |    }
         |  }
         |}
         |""".stripMargin
    )

  property("unconfigured firehose") {
    val runner = testConfig("")
    the[RuntimeException] thrownBy runner
      .getSinkConfig() should have message "kinesis stream name required but missing in sink <firehose-sink> of job <testJob>"
  }

  property("minimal configuration") {
    val runner = testConfig("stream = stream")
    noException should be thrownBy runner.getSinkConfig()
  }

  property("stream.name") {
    val runner = testConfig("stream.name = stream")
    noException should be thrownBy runner.getSinkConfig()
  }

  property("delivery.stream") {
    val runner = testConfig("delivery.stream = stream")
    noException should be thrownBy runner.getSinkConfig()
  }

  property("delivery.stream.name") {
    val runner = testConfig("delivery.stream.name = stream")
    noException should be thrownBy runner.getSinkConfig()
  }

  def getProps(config: String = ""): KinesisProperties =
    testConfig("stream = stream\n" + config)
      .getSinkConfig()
      .asInstanceOf[FirehoseSinkConfig[MySimpleADT]]
      .props

  def testProp[T](
      propList: Seq[String],
      prop: KinesisProperties => T,
      value: T): Unit =
    (propList ++ propList.map(p => s"config.$p")).foreach(c =>
      prop(getProps(s"$c = $value")) shouldEqual value
    )

  property("stream name") {
    getProps().stream shouldEqual "stream"
  }

  property("default aws.region") {
    getProps().clientProperties.getProperty(
      "aws.region"
    ) shouldEqual KinesisProperties.DEFAULT_REGION
  }

  property("aws.region") {
    testProp(
      Seq(
        "region",
        "aws.region"
      ),
      _.clientProperties.getProperty("aws.region"),
      Regions
        .values()
        .filterNot(_.getName != KinesisProperties.DEFAULT_REGION)
        .head
        .getName
    )
  }

  property("default aws.endpoint") {
    getProps().clientProperties
      .getProperty("aws.endpoint", "none") shouldEqual "none"
  }

  property("aws.endpoint") {
    testProp(
      Seq(
        "endpoint",
        "aws.endpoint"
      ),
      _.clientProperties.getProperty("aws.endpoint"),
      "other"
    )
  }

  property("default failOnError") {
    getProps().failOnError shouldBe KinesisProperties.DEFAULT_FAIL_ON_ERROR
  }

  property("failOnError") {
    testProp(
      Seq(
        "fail.on.error",
        "failOnError"
      ),
      _.failOnError,
      !KinesisProperties.DEFAULT_FAIL_ON_ERROR
    )
  }

  property("default maxInFlightRequests") {
    getProps().maxInFlightRequests shouldEqual KinesisProperties.DEFAULT_MAX_IN_FLIGHT_REQUESTS
  }

  property("maxInFlightRequests") {
    testProp(
      Seq(
        "maxInFlightRequests",
        "max.in.flight.requests"
      ),
      _.maxInFlightRequests,
      2 * KinesisProperties.DEFAULT_MAX_IN_FLIGHT_REQUESTS
    )
  }

  property("default maxBufferedRequests") {
    getProps().maxBufferedRequests shouldEqual KinesisProperties.DEFAULT_MAX_BUFFERED_REQUESTS
  }

  property("maxBufferedRequests") {
    testProp(
      Seq(
        "maxBufferedRequests",
        "max.buffered.requests"
      ),
      _.maxBufferedRequests,
      2 * KinesisProperties.DEFAULT_MAX_BUFFERED_REQUESTS
    )
  }

  property("default maxBatchSizeInNumber") {
    getProps().maxBatchSizeInNumber shouldEqual KinesisProperties.DEFAULT_MAX_BATCH_SIZE_IN_NUMBER
  }

  property("maxBatchSizeInNumber") {
    testProp(
      Seq(
        "maxBatchSizeInNumber",
        "max.batch.size.in.number",
        "max.batch.size.number"
      ),
      _.maxBatchSizeInNumber,
      2 * KinesisProperties.DEFAULT_MAX_BATCH_SIZE_IN_NUMBER
    )
  }

  property("default maxBatchSizeInBytes") {
    getProps().maxBatchSizeInBytes shouldEqual KinesisProperties.DEFAULT_MAX_BATCH_SIZE_IN_BYTES
  }

  property("maxBatchSizeInBytes") {
    testProp(
      Seq(
        "maxBatchSizeInBytes",
        "max.batch.size.in.bytes",
        "max.batch.size.bytes"
      ),
      _.maxBatchSizeInBytes,
      2 * KinesisProperties.DEFAULT_MAX_BATCH_SIZE_IN_BYTES
    )
  }

  property("default maxBufferTime") {
    getProps().maxBufferTime shouldEqual KinesisProperties.DEFAULT_MAX_BUFFER_TIME
  }

  property("maxBufferTime") {
    testProp(
      Seq(
        "maxBufferTime",
        "max.buffer.time"
      ),
      _.maxBufferTime,
      2 * KinesisProperties.DEFAULT_MAX_BUFFER_TIME
    )
  }

}
