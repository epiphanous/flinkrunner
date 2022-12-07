package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.PropSpec

import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import java.time.Instant

class UUIDSpec extends PropSpec {

  val rng = new SecureRandom()

  property("UUIDByteArray") {
    val bytes = new Array[Byte](16)
    rng.nextBytes(bytes)
    val u     = UUID.fromBytes(bytes)
    u.bytes shouldEqual bytes
  }

  val dataZeros: Array[Byte] =
    Array[Byte](0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
  val dataOnes: Array[Byte]  = Array[Byte](-1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1)

  property("UUIDByteIntByteArray") {
    var u = UUID.fromBytes(UUID.Variant_0_NCS, 0, dataZeros)
    u.getMostSignificantBits shouldEqual 0L
    u.getLeastSignificantBits shouldEqual 0L

    u = UUID.fromBytes(UUID.Variant_1_4122, 1, dataZeros)
    u.getMostSignificantBits shouldEqual 0x1000L
    u.getLeastSignificantBits shouldEqual 0x8000000000000000L

    u = UUID.fromBytes(UUID.Variant_1_4122, 4, dataOnes)
    u.getMostSignificantBits & 0xffff shouldEqual 0x4fffL
    u.getLeastSignificantBits >>> 32 shouldEqual 0xbfffffffL

    u = UUID.fromBytes(UUID.Variant_2_Microsoft, 0, dataOnes)
    u.getMostSignificantBits & 0xffff shouldEqual 0x0fffL
    u.getLeastSignificantBits >>> 32 shouldEqual 0xdfffffffL

    u = UUID.fromBytes(UUID.Variant_3_Future, 0, dataOnes)
    u.getMostSignificantBits & 0xffff shouldEqual 0x0fffL
    u.getLeastSignificantBits >>> 32 shouldEqual 0xffffffffL
  }

  property("UUIDLongLong") {
    val l1 = rng.nextLong()
    val l2 = rng.nextLong()
    val u  = UUID(l1, l2)
    u.getMostSignificantBits shouldEqual l1
    u.getLeastSignificantBits shouldEqual l2
  }

  property("RandomUUID") {
    val u  = UUID.randomUUID
    u.variant shouldEqual UUID.Variant_1_4122
    u.version shouldEqual 4
    val ju = new java.util.UUID(
      u.getMostSignificantBits,
      u.getLeastSignificantBits
    )
    ju.variant() shouldEqual 2
    ju.version() shouldEqual 4
  }

  property("TimeBasedUUID") {
    val u =
      UUID.timeBasedUUID(Instant.ofEpochSecond(1577872805).plusNanos(500))
    u.variant shouldEqual UUID.Variant_1_4122
    u.version shouldEqual 1
    u.timestamp shouldEqual 137971656050005L
    u.clockSequence shouldNot be(u.node)
  }

  property("Md5NameUUIDUUIDString") {
    val u = UUID.md5NameUUID(
      UUID.NameSpace_URL,
      "https://cloudonix.io/uuid-test"
    )
    u.toString shouldEqual "78b27cd6-ae27-3e33-919d-83a7e1d235f5"
  }

  property("Md5NameUUIDUUIDByteArray") {
    val u = UUID.md5NameUUID(
      UUID.NameSpace_URL,
      "https://cloudonix.io/uuid-test".getBytes(StandardCharsets.UTF_8)
    )
    u.toString shouldEqual "78b27cd6-ae27-3e33-919d-83a7e1d235f5"
  }

  property("Sha1NameUUIDUUIDString") {
    val u = UUID.sha1NameUUID(
      UUID.NameSpace_URL,
      "https://cloudonix.io/uuid-test"
    )
    u.toString shouldEqual "9f15406f-3afd-555d-85b7-ad3a6ff0b2e2"
  }

  property("Sha1NameUUIDUUIDByteArray") {
    val u = UUID.sha1NameUUID(
      UUID.NameSpace_URL,
      "https://cloudonix.io/uuid-test".getBytes(StandardCharsets.UTF_8)
    )
    u.toString shouldEqual "9f15406f-3afd-555d-85b7-ad3a6ff0b2e2"
  }

  property("FromString") {
    val u = UUID.fromString("078532d8-053f-4f95-9380-9f63d15e1d28")
    u.getMostSignificantBits shouldEqual 0x078532d8053f4f95L
    u.getLeastSignificantBits shouldEqual 0x93809f63d15e1d28L
  }

  property("FromJavaUUID") {
    val ju = java.util.UUID.randomUUID()
    val u  = UUID.fromJavaUUID(ju)
    u.toString shouldEqual ju.toString
  }

  property("ToJavaUUID") {
    val u  = UUID.randomUUID
    val ju = u.toJavaUUID
    u.toString shouldEqual ju.toString
  }

  property("getMostSignificantBites") {
    val l = rng.nextLong()
    val u = UUID(l, 0L)
    u.getMostSignificantBits shouldEqual l
  }

  property("getLeastSignificantBites") {
    val l = rng.nextLong()
    val u = UUID(0L, l)
    u.getLeastSignificantBits shouldEqual l
  }

  property("Variant") {
    val u1 = UUID.fromBytes(UUID.Variant_0_NCS, 0, UUID.randomUUID.bytes)
    u1.variant shouldEqual UUID.Variant_0_NCS

    val u2 = UUID.fromBytes(UUID.Variant_1_4122, 0, UUID.randomUUID.bytes)
    u2.variant shouldEqual UUID.Variant_1_4122

    val u3 =
      UUID.fromBytes(UUID.Variant_2_Microsoft, 0, UUID.randomUUID.bytes)
    u3.variant shouldEqual UUID.Variant_2_Microsoft

    val u4 =
      UUID.fromBytes(UUID.Variant_3_Future, 0, UUID.randomUUID.bytes)
    u4.variant shouldEqual UUID.Variant_3_Future

  }

  property("Version") {
    val u1 = UUID.fromBytes(UUID.Variant_1_4122, 1, UUID.randomUUID.bytes)
    u1.version shouldEqual 1

    val u2 = UUID.fromBytes(UUID.Variant_1_4122, 2, UUID.randomUUID.bytes)
    u2.version shouldEqual 2

    val u3 = UUID.fromBytes(UUID.Variant_1_4122, 3, UUID.randomUUID.bytes)
    u3.version shouldEqual 3

    val u4 = UUID.fromBytes(UUID.Variant_1_4122, 4, UUID.randomUUID.bytes)
    u4.version shouldEqual 4

    val u5 = UUID.fromBytes(UUID.Variant_1_4122, 5, UUID.randomUUID.bytes)
    u5.version shouldEqual 5
  }

  property("ToString") {
    val ju = java.util.UUID.randomUUID()
    val u  = UUID.fromJavaUUID(ju)
    u.toString shouldEqual ju.toString
  }

  property("ToStringRepresentation") {
    val uuidString = "fa0d68f3-882f-4c34-97b1-466e2fed93e9"
    val ju         = java.util.UUID.fromString(uuidString)
    val u          = UUID.fromJavaUUID(ju)
    u.toString shouldEqual ju.toString
  }

  property("EqualsObject") {
    val u1 = UUID.fromString("078532d8-053f-4f95-9380-9f63d15e1d28")
    val u2 = UUID(0x078532d8053f4f95L, 0x93809f63d15e1d28L)
    u1 shouldEqual u2
  }

  property("CompareTo") {
    val u1 = UUID.fromString("d5c6374f-6f89-4950-afa2-f5cbdb3ee7bf")
    val u2 = UUID.fromString("078532d8-053f-4f95-9380-9f63d15e1d28")
    val u3 = UUID(0x078532d8053f4f95L, 0x93809f63d15e1d28L)
    u1.compareTo(u2) shouldBe <(0)
    u2.compareTo(u1) shouldBe >(0)
    u2.compareTo(u3) shouldEqual 0
  }
}
