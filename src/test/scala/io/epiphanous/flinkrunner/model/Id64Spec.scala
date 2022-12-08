package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.PropSpec

class Id64Spec extends PropSpec {

  property("creates reversible ids of length 22") {
    val id   = Id64.gen()
    val uuid = Id64.ungen(id)
    val id2  = Id64.fromUUID(uuid)
    id shouldEqual id2
    id.length shouldEqual 22
  }

  property("uses valid characters only") {
    val chars = s"[${D64.chars}]{22}"
    val id    = Id64.gen()
    id should fullyMatch regex chars
  }

  property("creates non-reversible ids of length 20") {
    val id = Id64.gen(false)
    id.length shouldEqual 20
  }

  property("sortable") {
    val unsorted = Range(0, 100).map(_ => Id64.gen())
    unsorted shouldEqual unsorted.sorted
  }

  property("can extract time from id") {
    val id = Id64.gen()
    val t  = Id64.ticksOf(id)
    val mc = Id64.microsOf(id)
    val ms = Id64.millisOf(id)
    val i  = Id64.instantOf(id)
    val g  = Id64.GREGORIAN_OFFSET
    val e  = t - g
    mc shouldEqual Math.floor(e / 10)
    ms shouldEqual Math.floor(mc / 1000)
    i.getEpochSecond shouldEqual Math.floor(
      e / 10000000
    )
    i.getNano shouldEqual (e - 10000000 * i.getEpochSecond) * 100
  }

}
