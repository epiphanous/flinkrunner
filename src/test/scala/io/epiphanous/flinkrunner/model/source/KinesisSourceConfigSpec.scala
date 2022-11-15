package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.model.{FlinkConfig, MySimpleADT}

class KinesisSourceConfigSpec extends PropSpec {

  def getConfig(
      sourceConfigStr: String,
      jobName: String = "test"): KinesisSourceConfig[MySimpleADT] = {
    val configStr =
      s"""
         |jobs {
         |  $jobName {
         |    sources {
         |      kinesis-test {
         |        $sourceConfigStr
         |      }
         |    }
         |  }
         |}
         |""".stripMargin
    KinesisSourceConfig[MySimpleADT](
      "kinesis-test",
      new FlinkConfig(Array(jobName), Some(configStr))
    )
  }

  val requiredProps: String =
    """
      |stream = test
      |""".stripMargin

  def defaultConfig: KinesisSourceConfig[MySimpleADT] = getConfig(
    requiredProps
  )

  def defaultConfigPlus(
      str: String,
      job: String = "test"): KinesisSourceConfig[MySimpleADT] =
    getConfig(requiredProps + str, job)

  def noProvidedConfig: KinesisSourceConfig[MySimpleADT] = getConfig(
    "config={}"
  )

  property("default startPos property") {
    defaultConfig.startPos shouldEqual "LATEST"
  }

  property("bad startPos property") {
    the[Exception] thrownBy defaultConfigPlus(
      "start.pos = BAD_START_POS"
    ) should have message "Kinesis source kinesis-test has invalid `starting.position` <BAD_START_POS>. Instead, use one of TRIM_HORIZON, LATEST, AT_TIMESTAMP"
  }

  property("trim horizon starting.position property") {
    defaultConfigPlus(
      "starting.position = TRIM_HORIZON"
    ).startPos shouldEqual "TRIM_HORIZON"
  }

  property("start.pos property") {
    defaultConfigPlus(
      "start.pos = TRIM_HORIZON"
    ).startPos shouldEqual "TRIM_HORIZON"
  }

  property("start.pos=at_timestamp property") {
    the[Exception] thrownBy {
      defaultConfigPlus("""
                           |start.pos = AT_TIMESTAMP
                           |""".stripMargin)
    } should have message "kinesis sink kinesis-test set starting.position to AT_TIMESTAMP but provided no starting.timestamp"
  }

  property("efoConsumer property") {
    defaultConfigPlus(
      "efo.consumer = dogmo"
    ).efoConsumer shouldEqual "dogmo"

    defaultConfigPlus(
      "config.flink.stream.efo.consumer = dogmo"
    ).efoConsumer shouldEqual "dogmo"
  }

  property("useEfo true by default property") {
    defaultConfig.useEfo shouldBe true
  }

  property("useEfo property") {
    defaultConfigPlus("use.efo = true").useEfo shouldBe true
    defaultConfigPlus("use.efo = false").useEfo shouldBe false
    defaultConfigPlus("efo.enabled = false").useEfo shouldBe false
  }

  property("missing stream property") {
    the[Exception] thrownBy noProvidedConfig should have message "kinesis source kinesis-test is missing required 'stream' property"
  }

}
