package io.epiphanous.flinkrunner.util

import java.nio.ByteBuffer
import java.util.UUID

object UuidUtils {

  /** Name string is a fully-qualified domain name */
  val NameSpace_DNS = new UUID(
    /* 6ba7b810-9dad-11d1-80b4-00c04fd430c8 */ 0x6ba7b8109dad11d1L,
    0x80b400c04fd430c8L
  )

  /** Name string is a URL */
  val NameSpace_URL = new UUID(
    /* 6ba7b811-9dad-11d1-80b4-00c04fd430c8 */ 0x6ba7b8119dad11d1L,
    0x80b400c04fd430c8L
  )

  /** Name string is an ISO OID */
  val NameSpace_OID = new UUID(
    /* 6ba7b812-9dad-11d1-80b4-00c04fd430c8 */ 0x6ba7b8129dad11d1L,
    0x80b400c04fd430c8L
  )

  /** Name string is an X.500 DN (in DER or a text output format) */
  val NameSpace_X500 = new UUID(
    /* 6ba7b814-9dad-11d1-80b4-00c04fd430c8 */ 0x6ba7b8149dad11d1L,
    0x80b400c04fd430c8L
  )

  val Variant_0_NCS: Byte       = 0x0.toByte
  val Variant_1_4122: Byte      = 0x80.toByte
  val Variant_2_Microsoft: Byte = 0xc0.toByte
  val Variant_3_Future: Byte    = 0xe0.toByte

  implicit class RichUuid(uuid: UUID) {
    def bytes: Array[Byte] = {
      val buffer = ByteBuffer.allocate(16)
      buffer.putLong(uuid.getMostSignificantBits)
      buffer.putLong(uuid.getLeastSignificantBits)
      buffer.array()
    }
  }

}
