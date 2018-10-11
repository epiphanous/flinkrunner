package io.epiphanous.flinkrunner.operator
import com.github.ponkin.bloom.StableBloomFilter
import org.apache.flink.api.common.functions.RichAggregateFunction

class BloomFilterAggregateFunction(capacity: Long, bitsPerCell: Int = 3, falsePositiveRate: Double = 0.001)
    extends RichAggregateFunction[String, StableBloomFilter, StableBloomFilter] {

  override def createAccumulator() =
    StableBloomFilter
      .builder()
      .withExpectedNumberOfItems(capacity)
      .withBitsPerBucket(bitsPerCell)
      .withFalsePositiveRate(falsePositiveRate)
      .build()

  override def add(value: String, accumulator: StableBloomFilter) = {
    accumulator.put(value)
    accumulator
  }

  override def getResult(accumulator: StableBloomFilter) = accumulator

  override def merge(a: StableBloomFilter, b: StableBloomFilter) =
    a.mergeInPlace(b).asInstanceOf[StableBloomFilter]
}
