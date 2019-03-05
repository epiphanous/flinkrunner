package io.epiphanous.flinkrunner.operator
import java.nio.charset.StandardCharsets

import com.google.common.hash.Funnels
import io.epiphanous.flinkrunner.algorithm.membership.StableBloomFilter
import org.apache.flink.api.common.functions.RichAggregateFunction

class BloomFilterAggregateFunction(capacity: Long, bitsPerCell: Int = 3, falsePositiveRate: Double = 0.001)
    extends RichAggregateFunction[CharSequence, StableBloomFilter[CharSequence], StableBloomFilter[CharSequence]] {

  override def createAccumulator() =
    StableBloomFilter
      .builder(Funnels.stringFunnel(StandardCharsets.UTF_8))
      .withCapacity(capacity)
      .withBitsPerCell(bitsPerCell)
      .withFalsePositiveRate(falsePositiveRate)
      .build()

  override def add(value: CharSequence, accumulator: StableBloomFilter[CharSequence]) = {
    accumulator.add(value)
    accumulator
  }

  override def getResult(accumulator: StableBloomFilter[CharSequence]) = accumulator

  override def merge(a: StableBloomFilter[CharSequence], b: StableBloomFilter[CharSequence]) =
    a.merge(b)
}
