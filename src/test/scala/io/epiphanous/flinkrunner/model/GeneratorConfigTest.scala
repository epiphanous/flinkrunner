package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.PropSpec

import java.time.{Duration, Instant}
import java.util.Properties

class GeneratorConfigTest extends PropSpec {

  def gc: GeneratorConfig                   = GeneratorConfig("test", seedOpt = Some(123L))
  val firstBoolean: Boolean                 = gc.rng.nextBoolean()
  val firstString: String                   = gc.getRandString()
  def firstStringOfLength(len: Int): String = gc.getRandString(len)
  val firstDouble: Double                   = gc.rng.nextDouble()
  val firstInt: Int                         = gc.rng.nextInt()
  def firstBoundedInt(bound: Int): Int      = gc.rng.nextInt(bound)
  val firstLong: Long                       = gc.rng.nextLong()

  property("startTime property") {
    gc
      .copy(startAgo = Duration.ZERO)
      .startTime
      .minusMillis(Instant.now().toEpochMilli)
      .toEpochMilli shouldEqual 0L
  }

  property("timeSequence property") {
    gc
      .copy(startAgo = Duration.ZERO)
      .timeSequence
      .addAndGet(
        -Instant.now().toEpochMilli
      ) shouldEqual 0L +- 2L
  }

  property("maxRows property") {
    gc.copy(maxRows = 10).maxRows shouldEqual 10L
    gc.maxRows shouldEqual -1L
  }

  property("getRandInt bounded property") {
    gc.getRandInt(10) should (be >= 0 and be <= 10)
    gc.getRandInt(10) shouldEqual firstBoundedInt(10)
  }

  property("getRandInt property") {
    gc.getRandInt should (be >= Int.MinValue and be <= Int.MaxValue)
    gc.getRandInt shouldEqual firstInt
  }

  property("getProp property") {
    gc.getProp("not.there", 10).shouldEqual(10)
    val props = new Properties()
    props.setProperty("now.its.there", "10")
    gc.copy(properties = props).getProp("now.its.there", -1) shouldEqual 10
  }

  property("maxTimeStep property") {
    gc.maxTimeStep shouldEqual 100
    gc.copy(maxTimeStep = 250).maxTimeStep shouldEqual 250
  }

  property("seedOpt property") {
    gc.seedOpt.value shouldEqual 123L
  }

  property("probNull property") {
    gc.probNull shouldEqual 0
  }

  property("wantsNull property") {
    gc.wantsNull shouldEqual false
    gc.copy(probNull = firstDouble * .8).wantsNull shouldEqual false
    gc.copy(probNull = firstDouble * 1.2).wantsNull shouldEqual true
  }

  property("startAgo property") {
    gc.startAgo shouldEqual Duration.ofDays(365)
  }

  property("getRandString property") {
    gc.getRandString().length should be <= 20
    gc.getRandString(10).length should be <= 10
    gc.getRandString() should fullyMatch regex "[A-Za-z0-9]+"
    gc.getRandString() shouldEqual firstString
  }

  property("getRandBoolean property") {
    gc.getRandBoolean shouldEqual firstBoolean
  }

  property("getRandDouble property") {
    gc.getRandDouble shouldEqual firstDouble
  }

  property("getAndProgressTime property") {
    val myGc = gc
    myGc.getAndProgressTime() shouldEqual myGc.startTime.toEpochMilli
    myGc.getAndProgressTime() should be <= myGc.startTime
      .plusMillis(myGc.maxTimeStep)
      .toEpochMilli
  }

  property("probOutOfOrder property") {
    gc.probOutOfOrder shouldEqual 0
    gc.copy(probOutOfOrder = .5).probOutOfOrder shouldEqual .5
  }

}
