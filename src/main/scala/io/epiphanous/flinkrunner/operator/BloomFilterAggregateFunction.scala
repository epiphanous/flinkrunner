package io.epiphanous.flinkrunner.operator

import com.google.common.hash.Funnels
import io.epiphanous.flinkrunner.algorithm.membership.StableBloomFilter
import org.apache.flink.api.common.functions.RichAggregateFunction

import java.nio.charset.StandardCharsets

class BloomFilterAggregateFunction(
    numCells: Long,
    bitsPerCell: Int = 3,
    falsePositiveRate: Double = 0.001)
    extends RichAggregateFunction[
      CharSequence,
      StableBloomFilter[CharSequence],
      StableBloomFilter[CharSequence]] {

  override def createAccumulator(): StableBloomFilter[CharSequence] =
    StableBloomFilter
      .builder(Funnels.stringFunnel(StandardCharsets.UTF_8))
      .withNumCells(numCells)
      .withBitsPerCell(bitsPerCell)
      .withFalsePositiveRate(falsePositiveRate)
      .build()

  override def add(
      value: CharSequence,
      accumulator: StableBloomFilter[CharSequence])
      : StableBloomFilter[CharSequence] = {
    accumulator.add(value)
    accumulator
  }

  override def getResult(accumulator: StableBloomFilter[CharSequence])
      : StableBloomFilter[CharSequence] =
    accumulator

  override def merge(
      a: StableBloomFilter[CharSequence],
      b: StableBloomFilter[CharSequence])
      : StableBloomFilter[CharSequence] =
    a.merge(b)
}
