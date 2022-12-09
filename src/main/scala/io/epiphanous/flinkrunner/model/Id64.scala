package io.epiphanous.flinkrunner.model

import com.github.f4b6a3.uuid.UuidCreator
import io.epiphanous.flinkrunner.util.UuidUtils.RichUuid

import java.time.Instant
import java.util.UUID

/** Generates short identifiers that are time-sortable, url-safe,
  * lexicographically stable, and globally unique.
  *
  * Generated identifiers are by default 22 characters long. You can
  * recover the underlying time-based UUID from the string identifier.
  *
  * If you don't care about recovering the underlying UUID, you can
  * generate a 20 character id.
  *
  * Based on the original typescript implementation at
  * https://github.com/epiphanous/id64
  */
object Id64 {

  final val SHUFFLE_ORDER       =
    Array(6, 7, 4, 5, 0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14, 15)
  final val SHORT_SHUFFLE_ORDER = SHUFFLE_ORDER.filterNot(_ == 8)
  final val UNSHUFFLE_ORDER     =
    Array(4, 5, 6, 7, 2, 3, 0, 1, 8, 9, 10, 11, 12, 13, 14, 15)
  final val GREGORIAN_OFFSET    = 0x01b21dd213814000L

  def shuffle(bytes: Array[Byte], reversible: Boolean): Array[Byte] =
    if (reversible) SHUFFLE_ORDER.map(i => bytes(i))
    else SHORT_SHUFFLE_ORDER.map(i => bytes(i))

  def unshuffle(bytes: Array[Byte]): Array[Byte] =
    UNSHUFFLE_ORDER.map(i => bytes(i))

  def gen(reversible: Boolean = true): String =
    fromUUID(UuidCreator.getTimeBased(), reversible)

  def fromUUIDString(uuid: String, reversible: Boolean = true): String =
    fromUUID(UuidCreator.fromString(uuid), reversible)

  def fromUUID(uuid: UUID, reversible: Boolean = true): String = {
    val version = uuid.version()
    assert(
      version == 1,
      "ID64 requires time-based (v1) UUIDs to work"
    )
    D64.encode(shuffle(uuid.bytes, reversible))
  }

  def ungen(id: String): UUID = {
    val b = D64.decode(id)
    UuidCreator.fromBytes(unshuffle(if (b.length == 15) {
      val bb = Array.fill[Byte](16)(0)
      System.arraycopy(b, 0, bb, 0, 8)
      System.arraycopy(b, 8, bb, 9, 7)
      bb(8) = 17
      bb
    } else b))
  }

  def uuidOf(id: String): UUID = ungen(id)
  def toUUID(id: String): UUID = ungen(id)

  def ticksOf(id: String): Long      = uuidOf(id).timestamp()
  def microsOf(id: String): Long     =
    Math
      .floor((ticksOf(id) - GREGORIAN_OFFSET) / 10)
      .toLong
  def millisOf(id: String): Long     =
    Math
      .floor((ticksOf(id) - GREGORIAN_OFFSET) / 10000)
      .toLong
  def instantOf(id: String): Instant = {
    val t     = ticksOf(id) - GREGORIAN_OFFSET
    val secs  = t / 10000000
    val nsecs = (t - secs * 10000000) * 100
    Instant.ofEpochSecond(secs, nsecs)
  }

}
