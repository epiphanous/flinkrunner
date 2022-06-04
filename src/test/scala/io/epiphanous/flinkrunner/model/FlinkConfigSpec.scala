package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.{FlinkRunner, PropSpec}
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.scala.createTypeInformation

import java.time.Duration
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.util.Try

class FlinkConfigSpec extends PropSpec {
  val cmdLineArgs: Array[String]       =
    Array("someJob", "-a", "--arg1", "fish", "--arg2", "dog")
  val runner: FlinkRunner[NothingADT]  = getRunner[NothingADT](cmdLineArgs)
  val config: FlinkConfig              = runner.config
  val runner2: FlinkRunner[NothingADT] = getRunner[NothingADT](
    cmdLineArgs,
    Some("""
      |system.help = "system help"
      |execution.runtime-mode = batch
      |jobs {
      |  someJob {
      |    help = some job help
      |    description = some job description
      |    dish = 1
      |    my.string = dish
      |    my.string_list = [this, is, a, list, of, words]
      |    my.int = 17
      |    my.long = 123456789
      |    my.boolean = true
      |    my.double = 18.23
      |    my.duration = 3 days
      |  }
      |}
      |my.object = {
      |  x = 1
      |  y = 2
      |}
      |my.int = 17
      |my.string = "fish"
      |max.lateness = 90 seconds
      |max.idleness = 14 minutes
      |show.plan = false
      |environment = dev
      |mock.edges = true
      |""".stripMargin)
  )
  val config2: FlinkConfig             = runner2.config

  property("jobName") {
    config.jobName shouldEqual cmdLineArgs.head
  }

  property("jobArgs") {
    config.jobArgs shouldEqual cmdLineArgs.tail
  }

  property("jobParams") {
    config.jobParams.getNumberOfParameters shouldEqual 3
    config.jobParams.has("a") shouldEqual true
    config.jobParams.get("arg1", "-") shouldEqual "fish"
    config.jobParams.get("arg2", "-") shouldEqual "dog"
  }

  property("config precedence") {
    config.getString("test.precedence") shouldEqual "application.conf"
  }

  property("config precedence with file") {
    val configx = getRunner[NothingADT](
      cmdLineArgs :+ "--config" :+ "resource://test-precedence.conf"
    ).config
    configx.getString(
      "test.precedence"
    ) shouldEqual "test-precedence.conf"
  }

  property("config env variable") {
    // NOTE: this only works with sbt test, not intellij test (unless you set intellij env variable TEST_ENV_EXISTS=exists)
    config.getString("test.env.exists") shouldEqual "exists"
    config.getString("test.env.not.exists") shouldEqual "fallback"
  }

  property("system name") {
    config.systemName shouldEqual "test system name"
  }

  property("jobs") {
    config.jobs shouldEqual Set.empty[String]
    config2.jobs shouldEqual Set("someJob")
    config2.getJobConfig("someJob").hasPath("dish") shouldEqual true
  }

  property("missing throws") {
    Try(config2.getString("no.such.key")).failure
  }

  property("getObject") {
    val obj = config2.getObject("my.object")
    obj.unwrapped().asScala shouldEqual Map("x" -> 1, "y" -> 2)
  }

  property("getObjectObject") {
    val obj = config2.getObjectOption("my.object")
    obj.nonEmpty shouldBe true
    obj.value.unwrapped().asScala shouldEqual Map("x" -> 1, "y" -> 2)
  }

  property("getString") {
    config2.getString("my.string") shouldEqual "dish"
  }

  property("getStringOpt") {
    config2.getStringOpt("my.string").value shouldEqual "dish"
    config2.getStringOpt("no.such.key") shouldEqual None
  }

  property("getStringList") {
    config2.getStringList("my.string_list") shouldEqual List(
      "this",
      "is",
      "a",
      "list",
      "of",
      "words"
    )
  }
  property("getStringListOpt") {
    config2.getStringListOpt("my.string_list") shouldEqual List(
      "this",
      "is",
      "a",
      "list",
      "of",
      "words"
    )
    config2.getStringListOpt("my.no.such.string_list") shouldEqual List
      .empty[String]
  }

  property("getInt") {
    config2.getInt("my.int") shouldEqual 17
  }
  property("getIntOpt") {
    config2.getIntOpt("my.int").value shouldEqual 17
    config2.getIntOpt("my.no.such.int").isEmpty shouldEqual true
  }

  property("getLong") {
    config2.getLong("my.long") shouldEqual 123456789L
  }

  property("getLongOpt") {
    config2.getLongOpt("my.long").value shouldEqual 123456789L
    config2.getLongOpt("my.no.such.long").isEmpty shouldEqual true
  }

  property("getBoolean") {
    config2.getBoolean("my.boolean") shouldEqual true
  }
  property("getBooleanOpt") {
    config2.getBooleanOpt("my.boolean").value shouldEqual true
    config2.getBooleanOpt("my.no.such.boolean").isEmpty shouldEqual true
  }

  property("getDouble") {
    config2.getDouble("my.double") shouldEqual 18.23
  }
  property("getDoubleOpt") {
    config2.getDoubleOpt("my.double").value shouldEqual 18.23
    config2.getDoubleOpt("my.no.such.double").isEmpty shouldEqual true
  }

  property("getDuration") {
    config2.getDuration("my.duration") shouldEqual Duration
      .ofDays(3)
  }
  property("getDurationOpt") {
    config2.getDurationOpt("my.duration").value shouldEqual Duration
      .ofDays(3)
    config2.getDurationOpt("my.no.such.double").isEmpty shouldEqual true
  }

  property("getProperties") {
    val props = config2.getProperties("my.object")
    props.getOrDefault("x", "-") shouldEqual "1"
    props.getOrDefault("y", "-") shouldEqual "2"
    props.getOrDefault("z", "-") shouldEqual "-"
  }

  property("getSourceNames") {}
  property("getSinkNames") {}

  property("default test environment is dev") {
    config.environment shouldEqual "dev"
  }
  property("isDev") {
    getRunner[NothingADT](
      Array.empty[String],
      Some("environment = dev")
    ).config.isDev shouldEqual true
    getRunner[NothingADT](
      Array.empty[String],
      Some("environment = development")
    ).config.isDev shouldEqual true
    config2.isDev shouldEqual true
  }
  property("isStage") {
    getRunner[NothingADT](
      Array.empty[String],
      Some("environment = staging")
    ).config.isStage shouldEqual true
    getRunner[NothingADT](
      Array.empty[String],
      Some("environment = stage")
    ).config.isStage shouldEqual true
    config.isStage shouldEqual false
  }
  property("isProd") {
    getRunner[NothingADT](
      Array.empty[String],
      Some("environment = production")
    ).config.isProd shouldEqual true
    getRunner[NothingADT](
      Array.empty[String],
      Some("environment = prod")
    ).config.isProd shouldEqual true
    config.isProd shouldEqual false
    config2.isProd shouldEqual false
  }
  property("watermarkStrategy") {
    getRunner[NothingADT](
      Array.empty[String],
      Some("watermark.strategy = NONE")
    ).config.watermarkStrategy shouldEqual "none"
    config.watermarkStrategy shouldEqual "bounded out of orderness"
  }
  property("systemHelp") {
    config2.systemHelp shouldEqual "system help"
  }
  property("jobHelp") {
    config2.jobHelp shouldEqual "some job help"
  }
  property("jobDescription") {
    config2.jobDescription shouldEqual "some job description"
  }
  property("globalParallelism") {
    config2.globalParallelism shouldEqual 1
  }
  property("checkpointInterval") {
    config2.checkpointInterval shouldEqual Duration.ofSeconds(30).toMillis
  }
  property("checkpointMinPause") {
    config2.checkpointMinPause shouldEqual Duration.ofSeconds(10)
  }
  property("checkpointMaxConcurrent") {
    config2.checkpointMaxConcurrent shouldEqual 1
  }
  property("checkpointUrl") {
    config2.checkpointUrl shouldEqual "file:///tmp/checkpoint"
  }
  property("checkpointFlash") {
    config2.checkpointFlash shouldEqual false
  }
  property("checkpointIncremental") {
    config2.checkpointIncremental shouldEqual true
  }
  property("showPlan") {
    config2.showPlan shouldEqual false
  }
  property("mockEdges") {
    config.mockEdges shouldEqual true
    config2.mockEdges shouldEqual true
  }
  property("maxLateness") {
    config2.maxLateness shouldEqual Some(Duration.ofSeconds(90))
  }
  property("maxIdleness") {
    config2.maxIdleness shouldEqual Some(Duration.ofMinutes(14))
  }
  property("executionRuntimeMode") {
    config.executionRuntimeMode shouldEqual RuntimeExecutionMode.STREAMING
    config2.executionRuntimeMode shouldEqual RuntimeExecutionMode.BATCH
  }
  property("schemaRegistryUrl") {}
  property("schemaRegistryCacheCapacity") {}
  property("schemaRegistryProperties") {}
  property("schemaRegistryHeaders") {}
  property("schemaRegistryClient") {}
  property("schemaRegistryPropsForSource") {}
  property("schemaRegistryPropsForSink") {}
}
