package io.epiphanous.flinkrunner.algorithm.membership
import java.nio.ByteBuffer

import com.google.common.hash.Funnel
import com.google.common.hash.Hashing.murmur3_128

import scala.util.Random

/**
  * Implements the stable bloom filter from the paper by
  * F. Deng and D. Rafiei. <a href="">Approximately detecting
  * duplicates for streaming data using stable bloom
  * filters</a>. In SIGMOD, pages 25–36, 2006.
  *
  * We use heap storage (an array of Longs).
  * This implies <code>M=m*d</code> can be set as high as about 125 giga-bits.
  *
  * @param funnel a Guava funnel for taking input
  * @param m number of cells (see the paper, <code>m</code> is a <code>Long</code> but <code>m/floor(63/d)</code>
  *          must fit in a 32-bit <code>Int</code>)
  * @param d bits per cell (see the paper, should lie in [1,63] but often set to 1, 2 or 3)
  * @param FPR expected false positive rate (should lie in (0,1))
  * @tparam T the type of funnel used
  */
case class StableBloomFilter[T](funnel: Funnel[T], m: Long, d: Int, FPR: Double) {

  import StableBloomFilter._

  require(d > 0 && d <= STORAGE_BITS, s"d must be an integer in [1,$STORAGE_BITS]")

  /** number of bits used per unit storage */
  val storedBits: Long = STORAGE_BITS.toLong / (d * d)

  /** total memory required */
  val M = m * d

  require(M / storedBits < Int.MaxValue, s"M/$storedBits must be <= ${Int.MaxValue}")
  require(FPR > 0 && FPR < 1, "FPR must be a double in (0,1)")

  /** cell value to set upon insertion */
  val Max = (1 << d) - 1

  /** number of longs used for storage */
  val w = math.ceil(M.toDouble / storedBits).toInt

  /** number of hash functions used */
  val K = math.max(1, math.ceil(Max * LN2_SQUARED).toInt)

  /** number of cells to decrement on each insertion */
  val P = StableBloomFilter.optimalP(m, K, d, FPR)

  /** random number generator for decrementing cells */
  val random = new Random()

  /** murmur3 128 guava hashing function generator */
  val hasher = murmur3_128()

  /** heap storage for our bits */
  val storage = Array.fill[Long](w)(0)

  /**
    * Insert a stream element into the filter.
    * @param item the item to insert
    * @return
    */
  def add(item: T): Boolean = {
    val cells = hash(item)
    val alreadySeen = cells.forall(i => get(i) > 0L)
    decrementRandomCells()
    cells.foreach(set)
    alreadySeen
  }

  /**
    * Return true if this SBF might contain the requested item.
    * @param item the item to check
    * @return
    */
  def mightContain(item: T): Boolean =
    hash(item).forall(i => get(i) > 0L)

  /**
    * Merge another filter into this filter.
    * @param another the other filter
    * @return
    */
  def merge(another: StableBloomFilter[T]): StableBloomFilter[T] = {
    require(another.M == M && another.d == d && another.FPR == FPR, "Can only merge SBFs with same settings")
    another.storage.zipWithIndex.foreach { case (s, i) => storage(i) |= s }
    this
  }

  /**
    * Decrement P cells randomly. As recommended in the DR paper, we only generate a single random index, then
    * decrement that cell and the next <code>P-1</code> cells (wrapping around if needed).
    */
  private def decrementRandomCells(): Unit = {
    val p = (random.nextDouble() * m).toLong
    Range(0, P).map(i => (i + p) % m).foreach(decrement)
  }

  /**
    * Gets the current value of the <code>i</code>'th cell.
    * @param i the cell to get (in <code>[0, m)</code>)
    * @return
    */
  def get(i: Long) = {
    val (x, j) = offset(i)
    getBitsValue(x, j)
  }

  /**
    * Decrement a cell by one.
    * @param i the cell to decrement (in <code>[0,m)</code>)
    */
  private def decrement(i: Long): Unit = {
    val (x, j) = offset(i)
    val current = getBitsValue(x, j)
    if (current > 0)
      storage(x) -= (1L << j)
  }

  /**
    * Set a cell's value to Max
    * @param i the cell to set (in <code>[0,m)</code>)
    */
  private def set(i: Long): Unit = {
    val (x, j) = offset(i)
    storage(x) |= (Max.toLong << j)
  }

  /**
    * Extract the Int value of <code>d</code> bits (bits <code>j</code> to <code>j+d-1</code>) from stored element
    * <code>x</code>.
    * @param x the index into storage
    * @param j the LSB to start from
    * @return Int
    */
  private def getBitsValue(x: Int, j: Int) =
    (storage(x) & (Max.toLong << j)) >>> j

  /**
    * Converts a cell number into a tuple of <code>(x:Int, j:Int)</code>, allowing other methods to get and set
    * cell values.
    *
    * <code>x</code> in the integer offset within storage that contains cell <code>i</code>.
    * <code>j</code> is the relative offset (in [0,63]) of the LSB of cell <code>i</code> within <code>storage[x]</code>.
    * @param i the cell number in [0,m)
    * @return (Int, Int)
    */
  private def offset(i: Long): (Int, Int) = {
    // the cell covers d bits starting at b (within our total M bits)
    val b = (i - 1) * d
    // the b'th bit is stored in the x'th index of our storage array of longs
    val x = math.floor(b.toDouble / storedBits).toInt
    // from l, we're interested in d bits starting as LSB bit j
    val j = (b % storedBits).toInt
    // return l, x, and j
    (x, j)
  }

  /** Computes <code>K</code> hash functions of a filter item.
    * @param item the item to hash
    * @return
    */
  private def hash(item: T) = {
    val hash128 = hasher.hashObject(item, funnel).asBytes()
    val hash1 = ByteBuffer.wrap(hash128, 0, 8).getLong
    val hash2 = ByteBuffer.wrap(hash128, 8, 8).getLong
    Range(1, K + 1).map(
      i =>
        (hash1 + i * hash2 match {
          case combined if combined < 0 => ~combined
          case combined                 => combined
        }) % m
    )
  }

}

object StableBloomFilter {
  val STORAGE_BITS = java.lang.Long.SIZE - 1
  val LN2 = Math.log(2)
  val LN2_SQUARED = LN2 * LN2

  /** Return a builder for constructing an instance of StableBloomFilter[T] */
  def builder[T](funnel: Funnel[T]) = StableBloomFilterBuilder[T](funnel)

  /** Return the optimal number of cells to decrement each time a new item is inserted
    * in the filter. This quantity is represented by the symbol <code>P</code> in the DR paper (eqn 17).
    * @param m number of cells in the SBF
    * @param K number of hash functions
    * @param d bits per cell (<code>Max = 2**d - 1</code>)
    * @param FPS false positive rate
    * @return P optimal number of cells to decrement
    */
  def optimalP(m: Long, K: Int, d: Int, FPS: Double) = {

    val Max = (1L << d) - 1

    val mInverted = 1d / m
    val KInverted = 1d / K

    val denom1 = 1d / Math.pow(1 - Math.pow(FPS, KInverted), 1d / Max) - 1d
    val denom2 = KInverted - mInverted

    (1d / (denom1 * denom2)).toInt match {
      case x if x <= 0 => 1
      case x           => x
    }
  }
}
