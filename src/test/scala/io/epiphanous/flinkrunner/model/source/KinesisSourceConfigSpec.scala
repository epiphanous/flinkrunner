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

  val requiredProps1: String =
    """
      |streams = test
      |""".stripMargin

  val requiredPropsMulti1: String =
    """
      |stream = "test1, test2"
      |""".stripMargin

  val requiredPropsMulti2: String =
    """
      |streams = [test1, test2]
      |""".stripMargin

  def defaultConfig(
      reqProps: String = requiredProps): KinesisSourceConfig[MySimpleADT] =
    getConfig(
      reqProps
    )

  def defaultConfigPlus(
      str: String,
      job: String = "test",
      reqProps: String = requiredProps): KinesisSourceConfig[MySimpleADT] =
    getConfig(reqProps + str, job)

  def noProvidedConfig: KinesisSourceConfig[MySimpleADT] = getConfig(
    "config={}"
  )

  property("default startPos property") {
    defaultConfig().startPos shouldEqual "LATEST"
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
    defaultConfig().useEfo shouldBe true
  }

  property("useEfo property") {
    defaultConfigPlus("use.efo = true").useEfo shouldBe true
    defaultConfigPlus("use.efo = false").useEfo shouldBe false
    defaultConfigPlus("efo.enabled = false").useEfo shouldBe false
  }

  property("parallelism property") {
    defaultConfigPlus("parallelism = 10").parallelism shouldBe 10
    defaultConfigPlus("parallelism = 10.5").parallelism shouldBe 10
  }

  property("multi streams via stream property") {
    defaultConfig(requiredPropsMulti1).streams shouldBe Seq(
      "test1",
      "test2"
    )
  }

  property("multi streams via streams property") {
    defaultConfig(requiredPropsMulti2).streams shouldBe List(
      "test1",
      "test2"
    )
  }

  property("single stream via streams property") {
    defaultConfig(requiredProps1).streams shouldBe List(
      "test"
    )
  }

  property("single stream via stream property") {
    defaultConfig(requiredProps).streams shouldBe List(
      "test"
    )
  }

  property("missing stream property") {
    the[Exception] thrownBy noProvidedConfig should have message "kinesis source kinesis-test is missing required 'stream' property"
  }

}
