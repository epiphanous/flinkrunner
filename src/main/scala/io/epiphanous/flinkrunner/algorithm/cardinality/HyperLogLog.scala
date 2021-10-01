package io.epiphanous.flinkrunner.algorithm.cardinality

import com.google.common.hash.Funnel
import com.google.common.hash.Hashing.murmur3_128

/**
 * Implements hyperloglog cardinality estimate based on paper by P.
 * Flajolet, È. Fusy, O. Gandouet, F. Meiunier. HyperLogLog: the analysis
 * of a near-optimal cardinality estimation algorithm. Proceedings of
 * Discrete Mathematics and Theoretical Computer Science. Pages 127-146.
 * 2007.
 */
case class HyperLogLog[T](funnel: Funnel[T], b: Int) {

  require(b >= 4 && b <= 16, "b must be an integer in [4,16]")

  import HyperLogLog._

  /** number of registers <code>m = 2**b</code> */
  val m = 1 << b

  /** relativeError of cardinality estimates */
  val relativeError = 1.04 / math.sqrt(m.toDouble)

  /** correction constant <code>alpha(m) * m**2</code> */
  val am2 = ALPHA_M * m * m

  /** upper bound of small range */
  val smallRange = 5 / 2 * m

  /** upper bound of intermediate range */
  val intermediateRange = math.floor(TWO32 / 30).toInt

  /** registers */
  val M = Array.fill[Int](m)(0)

  /** murmur3 128 guava hashing function generator */
  val hasher = murmur3_128()

  /** current cardinality estimate */
  var cardinality = 0L

  /** True if no data has been added to the registers */
  def isEmpty = cardinality == 0

  /** True if data has been added to the registers */
  def nonEmpty = cardinality > 0

  /**
   * Incorporates an item into the registers, updates the cardinality
   * estimate and returns it.
   *
   * @param item
   *   the item to add
   * @return
   *   Long
   */
  def add(item: T) = {
    val x = hash(item)
    val j = 1 + (x & (m - 1))
    val w = x >> b
    M(j) = math.max(M(j), rho(w))
    estimateCardinality
  }

  /**
   * Compute the current distinct cardinality estimate.
   *
   * @return
   *   Long
   */
  private def estimateCardinality: Long = {
    val E     = am2 / M.map(i => 1 / math.pow(2d, i.toDouble)).sum
    // small range correction
    val Estar = if (E <= smallRange) {
      val V = M.count(_ == 0)
      if (V != 0) m * math.log(m.toDouble / V) else E
    }
    // intermediate range = no correction
    else if (E <= intermediateRange) E
    // large range correction
    else {
      -TWO32 * math.log(1d - E / TWO32)
    }
    cardinality = Estar.toLong
    cardinality
  }

  /**
   * Merge another HyperLogLog[T] instance into this instance. Note the
   * other instance must have the same b parameter as this instance.
   *
   * @param another
   *   the other HyperLogLog[T] instance
   */
  def merge(another: HyperLogLog[T]) = {
    if (another.nonEmpty) {
      require(another.m == m, s"Can only merge HLL with same b=$b")
      another.M.zipWithIndex.foreach { case (other, i) =>
        if (M(i) < other) M(i) = other
      }
      estimateCardinality
    }
    this
  }

  /**
   * Computes positive integer hash of item
   *
   * @param item
   *   item to hash
   * @return
   *   Int
   */
  private def hash(item: T): Int = {
    val h = hasher.hashObject(item, funnel).asInt()
    if (h < 0) ~h else h
  }

  /**
   * Computes most significant set bit of an integer, where returned bit in
   * [0,32].
   *
   * @param i
   *   the non-negative Int to examine
   * @return
   *   Int
   */
  private def rho(i: Int): Int = {
    require(i >= 0, "i must be non-negative integer")
    (32 - HyperLogLog.MASKS.lastIndexWhere(_ <= i)) % 33
  }
}

object HyperLogLog {
  val MASKS   = Range(1, 32).map(i => 1 << (i - 1))
  val ALPHA_M = 1 / (2 * math.log(2))
  val TWO32   = math.pow(2, 32)

}
