package io.epiphanous.flinkrunner.util

import io.epiphanous.flinkrunner.UnitSpec
import org.apache.flink.api.common.eventtime.{Watermark, WatermarkOutput}

import java.util.UUID.randomUUID
import scala.util.Random

class BoundedLatenessGeneratorTest extends UnitSpec {

  val now    = System.currentTimeMillis()
  val random = new Random()

  def getBlg(maxAllowedLateness: Long = 10L, streamID: String = "Test") =
    new BoundedLatenessGenerator[TestEvent](maxAllowedLateness, streamID)

  def uuid = randomUUID().toString

  def nextEvent(ts: Long) = TestEvent(uuid, ts)

  def ascending(space: Long = 5) = (prev: Long) => prev + space

  def randomWalk(minSpace: Long = -20, maxSpace: Long = 40) =
    (prev: Long) =>
      prev + Math.ceil(minSpace + random.nextDouble() * maxSpace).toLong

  def events(
      start: Long = now - 1000L,
      progress: Long => Long = randomWalk(),
      probSpike: Double = 0,
      spikeSize: Long = 50000
  ) =
    Stream
      .iterate((TestEvent(uuid, start), start)) { case (_, timeline) =>
        val spike = if (random.nextDouble() < probSpike) spikeSize else 0L
        val ts    = progress(timeline)
        (nextEvent(ts + spike), ts)
      }
      .map(_._1)

  def ascendingEvents(
      start: Long = now - 1000L,
      space: Long = 5,
      probSpike: Double = 0,
      spikeSize: Long = 50000) =
    events(start, ascending(space), probSpike, spikeSize)

  def randomEvents(
      start: Long = now - 1000L,
      minSpace: Long = -20,
      maxSpace: Long = 40,
      probSpike: Double = 0,
      spikeSize: Long = 50000
  ) =
    events(start, randomWalk(minSpace, maxSpace), probSpike, spikeSize)

  def randomEventsWithSpike(
      start: Long = now - 1000L,
      minSpace: Long = -20,
      maxSpace: Long = 40,
      probSpike: Double = .20,
      spikeSize: Long = 50000
  ) =
    randomEvents(start, minSpace, maxSpace, probSpike, spikeSize)

  behavior of "BoundedLatenessGenerator"

  it should "extract ascending timestamps from events" in {
    val blg        = getBlg()
    val space      = 5L
    val result     = ascendingEvents(space = space)
      .take(10)
      .map { e =>
        blg.onEvent(e, e.timestamp, null)
        (e.timestamp, blg.mostRecentTimestamp)
      }
    assert(result.forall { p =>
      println(p)
      p._1 === p._2
    })
    val seenSpaces =
      result.map(_._1).sliding(2).map { case Seq(x, y, _*) => y - x }.toSet
    assert(seenSpaces.size === 1)
    assert(seenSpaces.head === space)
  }

  def watermarkTest(testEvents: Stream[TestEvent]) = {
    var prevTs      = -1L
    val maxLateness = 10L
    val blg         = getBlg(maxLateness)
    val wmo         = new WatermarkOutput {
      override def emitWatermark(watermark: Watermark): Unit = {}
      override def markIdle(): Unit = {}
    }
    var maxTs       = 0L
    val result      = testEvents
      .take(10)
      .map { e =>
        maxTs =
          if (e.timestamp > System.currentTimeMillis()) maxTs
          else Math.max(maxTs, e.timestamp)
        val expectedWm = Math.max(maxTs - maxLateness - 1, 0)
        blg.onEvent(e, prevTs, wmo)
        blg.onPeriodicEmit(wmo)
        prevTs = blg.mostRecentTimestamp
        (expectedWm, blg.watermark)
      }
      .toList
    //    println(result.map(p => s"${p._1} | ${p._2.getTimestamp}").mkString("\n"))
    assert(result.forall { p =>
      p._1 == p._2.getTimestamp
    })
  }

  it should "get current watermark ascending events" in {
    watermarkTest(ascendingEvents())
  }

  it should "get current watermark randomWalk events" in {
    watermarkTest(randomEvents())
  }

  it should "get current watermark future events" in {
    watermarkTest(randomEventsWithSpike())
  }

}
