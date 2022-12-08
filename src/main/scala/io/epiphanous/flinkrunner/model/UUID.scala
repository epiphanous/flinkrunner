package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.model.UUID._

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.{MessageDigest, SecureRandom}
import java.time.{Duration, Instant, ZoneOffset, ZonedDateTime}
import java.util

/** Adapted for scala from https://github.com/guss77/java-uuid/ which is
  * under an MIT license
  *
  * @param msb
  *   most significant bits
  * @param lsb
  *   least significant bits
  */
case class UUID(msb: Long, lsb: Long) {
  lazy val javaUUID: util.UUID = new util.UUID(msb, lsb)
  def toJavaUUID: util.UUID    = javaUUID

  def getMostSignificantBits: Long  = msb
  def getLeastSignificantBits: Long = lsb

  lazy val version: Int = ((msb >>> 12) & 0x0f).toInt

  lazy val variant: Byte = {
    val clock_seq_hi_and_reserved = (lsb >>> 56).toByte
    if (clock_seq_hi_and_reserved > 0)
      Variant_0_NCS
    else if ((clock_seq_hi_and_reserved & 0xc0) == 0x80)
      Variant_1_4122
    else if ((clock_seq_hi_and_reserved & 0xe0) == 0xc0)
      Variant_2_Microsoft
    else Variant_3_Future
  }

  lazy val time_low: Long  = msb >>> 32
  lazy val time_mid: Long  = (msb >> 16) & 0xffffL
  lazy val time_high: Long = msb & 0x0fffL
  lazy val timestamp: Long =
    (time_high << 48) | (time_mid << 32) | (msb >>> 32)

  lazy val clockSequence: Int = ((lsb >>> 48) & variantMask(variant)).toInt
  lazy val node: Long         = lsb & 0x0000ffffffffffffL
  lazy val bytes: Array[Byte] = {
    val buffer = ByteBuffer.allocate(16)
    buffer.putLong(msb)
    buffer.putLong(lsb)
    buffer.array()
  }

  override def toString: String = javaUUID.toString

  def compareTo(other: UUID): Int = {
    val comp = java.lang.Long.compare(msb, other.msb)
    if (comp == 0) java.lang.Long.compare(lsb, other.lsb) else comp
  }
}

object UUID {

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
  val NameSpace_X500            = new UUID(
    /* 6ba7b814-9dad-11d1-80b4-00c04fd430c8 */ 0x6ba7b8149dad11d1L,
    0x80b400c04fd430c8L
  )
  val Variant_0_NCS: Byte       = 0x0.toByte
  val Variant_1_4122: Byte      = 0x80.toByte
  val Variant_2_Microsoft: Byte = 0xc0.toByte
  val Variant_3_Future: Byte    = 0xe0.toByte

  /* lazy initialized RNG for v4 UUIDs */
  object RandHolder {
    val rng                          = new SecureRandom
    val current_clock_sequence: Long = rng.nextLong
    val node_id                      = new Array[Byte](6)
    rng.nextBytes(node_id)
  }

  /** Create v4 (pseudo randomly generated) UUID. Random bits are generated
    * using a statically initialized [[SecureRandom]] instance.
    *
    * @return
    *   Version 4 (type 4) randomly generated UUID
    */
  def randomUUID: UUID = {
    val data = new Array[Byte](16)
    RandHolder.rng.nextBytes(data)
    fromBytes(Variant_1_4122, 4, data)
  }

  def fromBytes(variant: Byte, version: Int, data: Array[Byte]): UUID = {
    assert(data.length >= 16, "data must be 16 bytes in length")
    if ((variant & 0x80) == 0) { // NCS backward compatibility
      data.update(8, (data(8) & 0x7f).toByte)
    } else if ((variant & 0xc0) >> 6 == 2) { // RFC 4122 current version
      data.update(8, ((data(8) & 0x3f) | 0x80).toByte)
    } else if ((variant & 0xe0) >> 5 == 6) { // Microsoft GUID mixed-endianess format
      data.update(8, ((data(8) & 0x1f) | 0xc0).toByte)
    } else if ((variant & 0xe0) >> 5 == 7) { // Future-reserved
      data.update(8, (data(8) | 0xe0).toByte)
    }
    data(6) = ((data(6) & 0x0f) | ((version & 0x0f) << 4)).toByte
    fromBytes(data)
  }

  def fromBytes(data: Array[Byte]): UUID = {
    assert(data.length >= 16, "data must be 16 bytes in length")

    var msb = 0L
    var lsb = 0L
    for (i <- 0 until 8)
      msb = (msb << 8) | (data(i) & 0xff)
    for (i <- 8 until 16)
      lsb = (lsb << 8) | (data(i) & 0xff)
    UUID(msb, lsb)
  }

  /** Create v1 (time-based) UUID from the specified time instance (default
    * now). The clock sequence and node ID are randomly generated using a
    * statically initialized [[SecureRandom]] instance, and are kept the
    * same for the life time of this class (normally the lifetime of the
    * class loader).
    *
    * @param time
    *   time instant for which to generate a timestamp UUID
    * @return
    *   Version 1 (type 1) time-based UUID with the specified timestamp and
    *   random clock sequence and node ID.
    */
  def timeBasedUUID(time: Instant = Instant.now()): UUID = {
    val dur       = Duration.between(
      ZonedDateTime.of(1582, 10, 15, 0, 0, 0, 0, ZoneOffset.UTC),
      time.atZone(ZoneOffset.UTC)
    )
    var timestamp = dur.getSeconds * 10000000 + dur.getNano / 100
    val time_low  = timestamp & 0xffffffffL
    timestamp >>>= 32
    val time_mid  = timestamp & 0xffffL
    timestamp >>>= 16
    val time_high = timestamp & 0xfff
    val msb       = time_low << 32 | time_mid << 16 | time_high
    val data      = ByteBuffer.allocate(16)
    data.putLong(msb)
    data.putShort((RandHolder.current_clock_sequence & 0xffff).toShort)
    data.put(RandHolder.node_id)
    fromBytes(Variant_1_4122, 1, data.array)
  }

  /** Create v3 (MD5-hashed) UUID with the specified namespace and content.
    * The namespace is assumed to be one of the <pre>Namespace_*</pre>
    * constants defined in the class - but it doesn't have to be and any
    * namespace UUID can be specified and will generate a consistent
    * result.
    *
    * @param namespace
    *   UUID of namespace in which to generate the UUID
    * @param name
    *   content from which to generate the UUID - the text is UTF-8 encoded
    *   into the content data
    * @return
    *   Version 3 (type 3) MD5 hash based UUID with the specified namespace
    *   and name
    */
  def md5NameUUID(namespace: UUID, name: String): UUID =
    md5NameUUID(namespace, name.getBytes(StandardCharsets.UTF_8))

  /** Create v3 (MD5-hashed) UUID with the specified namespace and content.
    * The namespace is assumed to be one of the <pre>Namespace_*</pre>
    * constants defined in the class - but it doesn't have to be and any
    * namespace UUID can be specified and will generate a consistent
    * result.
    *
    * @param namespace
    *   UUID of namespace in which to generate the UUID
    * @param name
    *   content from which to generate the UUID
    * @return
    *   Version 3 (type 3) MD5 hash based UUID with the specified namespace
    *   and name
    */
  def md5NameUUID(namespace: UUID, name: Array[Byte]): UUID =
    _nameUUID(namespace, name, MessageDigest.getInstance("MD5"), 3)

  /** Create v5 (SHA1-hashed) UUID with the specified namespace and
    * content. The namespace is assumed to be one of the
    * <pre>Namespace_*</pre> constants defined in the class - but it
    * doesn't have to be and any namespace UUID can be specified and will
    * generate a consistent result.
    *
    * @param namespace
    *   UUID of namespace in which to generate the UUID
    * @param name
    *   content from which to generate the UUID - the text is UTF-8 encoded
    *   into the content data
    * @return
    *   Version 5 (type 5) SHA1 hash based UUID with the specified
    *   namespace and name
    */
  def sha1NameUUID(namespace: UUID, name: String): UUID =
    sha1NameUUID(namespace, name.getBytes(StandardCharsets.UTF_8))

  /** Create v5 (SHA1-hashed) UUID with the specified namespace and
    * content. The namespace is assumed to be one of the
    * <pre>Namespace_*</pre> constants defined in the class - but it
    * doesn't have to be and any namespace UUID can be specified and will
    * generate a consistent result.
    *
    * @param namespace
    *   UUID of namespace in which to generate the UUID
    * @param name
    *   content from which to generate the UUID
    * @return
    *   Version 5 (type 5) SHA1 hash based UUID with the specified
    *   namespace and name
    */
  def sha1NameUUID(namespace: UUID, name: Array[Byte]): UUID =
    _nameUUID(namespace, name, MessageDigest.getInstance("SHA1"), 5)

  def _nameUUID(
      namespace: UUID,
      name: Array[Byte],
      digest: MessageDigest,
      version: Int): UUID = {
    val buffer = ByteBuffer.allocate(name.length + 16)
    buffer.putLong(namespace.msb)
    buffer.putLong(namespace.lsb)
    buffer.put(name)
    buffer.flip
    digest.update(buffer)
    fromBytes(Variant_1_4122, version, digest.digest)
  }

  /** Create a UUID instance by parsing the UUID string representation
    * provided This method does not verify the RFC 4122 compliance
    * according to the variant and version values - it just reads the bit
    * data encoded in the string representation
    *
    * @param uuid
    *   String representation of a UUID according to RFC 4122
    * @return
    *   UUID containing the data encoded in the string
    * @throws IllegalArgumentException
    *   if the provided string is not a valid RFC 4122 string
    *   representation
    */
  @throws[IllegalArgumentException]
  def fromString(uuid: String): UUID = try {
    val longs                 = uuid.split("-").map(s => java.lang.Long.parseLong(s, 16))
    if (longs.length != 5)
      throw new NumberFormatException("missing dashes")
    val time_low              = longs(0)
    val time_mid              = longs(1)
    val time_high_and_version = longs(2)
    val clock_seq             = longs(3)
    val node                  = longs(4)
    UUID(
      time_low << 32 | time_mid << 16 | time_high_and_version,
      clock_seq << 48 | node
    )
  } catch {
    case e: NumberFormatException =>
      throw new IllegalArgumentException("Invalid UUID string: " + uuid, e)
  }

  /** Convert a <pre>java.util.UUID</pre> to our UUID instance
    *
    * @param uuid
    *   Java UUID class
    * @return
    *   a new UUID containing the same data as the provided Java UUID
    */
  def fromJavaUUID(uuid: util.UUID): UUID =
    UUID(uuid.getMostSignificantBits, uuid.getLeastSignificantBits)

  /** Compute the bits in clock_seq not used by the variant data, for each
    * supported variant
    *
    * @param variant
    *   a supported variant constant value from one of the `Variant_*`
    *   constants;
    * @return
    *   the map to apply to zero the correct number of bits needed to store
    *   the variant data
    */
  private def variantMask(variant: Byte) = variant match {
    case Variant_0_NCS  =>
      0x7fff.toShort
    case Variant_1_4122 =>
      0x3fff.toShort
    case _              =>
      0x1fff.toShort
  }
}
