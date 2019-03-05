package io.epiphanous.flinkrunner.algorithm.membership
import com.google.common.hash.Funnel

/**
  * A builder interface for creating [[StableBloomFilter]] instances.
  *
  * @param funnel a guava funnel
  * @param capacity number of total bits to make available
  * @param bitsPerCell number of bits per cell in the filter
  * @param falsePositiveRate desired maximum false positive rate of the filter
  * @tparam T the type of item inserted into the filter
  */
case class StableBloomFilterBuilder[T](
  funnel: Funnel[T],
  capacity: Long = 1024 * 1024 * 8L,
  bitsPerCell: Int = 3,
  falsePositiveRate: Double = 0.01) {
  def withCapacity(n: Long) = copy(capacity = n)
  def withFalsePositiveRate(p: Double) = copy(falsePositiveRate = p)
  def withBitsPerCell(d: Int) = copy(bitsPerCell = d)
  def build() = StableBloomFilter(funnel, capacity, bitsPerCell, falsePositiveRate)
}
