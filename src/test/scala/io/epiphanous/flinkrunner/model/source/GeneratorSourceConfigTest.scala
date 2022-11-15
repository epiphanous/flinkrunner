package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.model._
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant}

class GeneratorSourceConfigTest extends PropSpec {

  def getSourceConfig(
      configStr: String): GeneratorSourceConfig[MySimpleADT] =
    GeneratorSourceConfig(
      "generator-test",
      new FlinkConfig(Array("test"), Some(configStr)),
      new TestGeneratorFactory
    )

  property("basic getAndProgressTime property") {
    val sc    = getSourceConfig(s"""
         |sources {
         |  generator-test {
         |    start.ago = 3d
         |  }
         |}
         |""".stripMargin)
    val gc    = sc.generatorConfig
    val t0    = gc.getAndProgressTime()
    val t1    = gc.getAndProgressTime()
    val start = gc.startTime
    val step  = gc.maxTimeStep.toLong
    (t0 - start.toEpochMilli) should be < 1L
    (t1 - t0) should be >= 0L
    (t1 - t0) should be <= step
  }

  property("getAndProgressTime out of order property") {
    val sc    = getSourceConfig(s"""
         |sources {
         |  generator-test {
         |    start.ago = 3d
         |    prob.out.of.order = 1
         |  }
         |}
         |""".stripMargin)
    val gc    = sc.generatorConfig
    val t0    = gc.getAndProgressTime()
    val t1    = gc.getAndProgressTime()
    val start = gc.startTime
    val step  = gc.maxTimeStep.toLong
    (t0 - start.toEpochMilli) shouldEqual 0L
    (t1 - t0) should be <= 0L
    Math.abs(t1 - t0) should be <= step
  }

  property("getSource no generator property") {
    val sc  = getSourceConfig("""
        |sources {
        |  generator-test {
        |    start.ago = 3d
        |  }
        |}
        |""".stripMargin)
    val src = sc.getSource[SimpleA]
    src.left.value shouldBe a[DataGeneratorSource[SimpleA]]
  }

  property("seedOpt property") {
    val sc = getSourceConfig("""
        |sources {
        |  generator-test {
        |    start.ago = 3d
        |    seed = 123
        |  }
        |}
        |""".stripMargin)
    sc.generatorConfig.seedOpt.value shouldEqual 123L
  }

  property("rowsPerSecond property") {
    val sc = getSourceConfig("""
        |sources {
        |  generator-test {
        |    start.ago = 3d
        |    rows.per.second = 123
        |  }
        |}
        |""".stripMargin)
    sc.generatorConfig.rowsPerSecond shouldEqual 123L
  }

  property("maxRows property") {
    val sc = getSourceConfig("""
        |sources {
        |  generator-test {
        |    start.ago = 3d
        |    max.rows = 123
        |  }
        |}
        |""".stripMargin)
    sc.generatorConfig.maxRows shouldEqual 123L
  }

  property("bounded property") {
    val sc = getSourceConfig("""
        |sources {
        |  generator-test {
        |    start.ago = 3d
        |    max.rows = 123
        |  }
        |}
        |""".stripMargin)
    sc.isBounded shouldEqual true
  }

  property("unbounded property") {
    val sc = getSourceConfig("""
        |sources {
        |  generator-test {
        |    start.ago = 3d
        |  }
        |}
        |""".stripMargin)
    sc.isBounded shouldEqual false
  }

  property("startTime property") {
    val sc     = getSourceConfig("""
        |sources {
        |  generator-test {
        |    start.ago = 3d
        |  }
        |}
        |""".stripMargin)
    val before =
      Instant
        .now()
        .minus(Duration.of(73, ChronoUnit.HOURS))
    val after  =
      Instant
        .now()
        .minus(Duration.of(71, ChronoUnit.HOURS))
    before.isBefore(sc.generatorConfig.startTime) shouldEqual true
    after.isAfter(sc.generatorConfig.startTime) shouldEqual true
  }

  property("maxTimeStep property") {
    val sc = getSourceConfig("""
        |sources {
        |  generator-test {
        |    start.ago = 3d
        |    max.time.step.millis = 123
        |  }
        |}
        |""".stripMargin)
    sc.generatorConfig.maxTimeStep shouldEqual 123
  }

  property("prob out of order property") {
    val sc = getSourceConfig("""
        |sources {
        |  generator-test {
        |    start.ago = 3d
        |    prob.out.of.order = .5
        |  }
        |}
        |""".stripMargin)
    sc.generatorConfig.probOutOfOrder shouldEqual 0.5
  }

  property("prob null property") {
    val sc = getSourceConfig("""
        |sources {
        |  generator-test {
        |    start.ago = 3d
        |    prob.null = .5
        |  }
        |}
        |""".stripMargin)
    sc.generatorConfig.probNull shouldEqual 0.5
  }

}
