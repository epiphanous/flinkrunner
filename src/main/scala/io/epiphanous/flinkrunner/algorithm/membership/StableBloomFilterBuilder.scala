package io.epiphanous.flinkrunner.algorithm.membership

import com.google.common.hash.Funnel

/** A builder interface for creating StableBloomFilter instances.
  *
  * @param funnel
  *   a guava funnel
  * @param numCells
  *   number of cells in the filter
  * @param bitsPerCell
  *   number of bits per cell in the filter
  * @param falsePositiveRate
  *   desired maximum false positive rate of the filter
  * @tparam T
  *   the type of item inserted into the filter
  */
case class StableBloomFilterBuilder[T](
    funnel: Funnel[T],
    numCells: Long = 1000000,
    bitsPerCell: Int = 3,
    falsePositiveRate: Double = 0.01) {
  def withNumCells(m: Long): StableBloomFilterBuilder[T] =
    copy(numCells = m)

  def withBitsPerCell(d: Int): StableBloomFilterBuilder[T] =
    copy(bitsPerCell = d)

  def withFalsePositiveRate(p: Double): StableBloomFilterBuilder[T] =
    copy(falsePositiveRate = p)

  def build(): StableBloomFilter[T] =
    StableBloomFilter(funnel, numCells, bitsPerCell, falsePositiveRate)
}
