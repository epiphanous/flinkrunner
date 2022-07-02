package io.epiphanous.flinkrunner.operator

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.algorithm.membership.StableBloomFilter
import io.epiphanous.flinkrunner.model.FlinkEvent
import io.epiphanous.flinkrunner.model.source.SourceConfig
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.common.state.{
  AggregatingState,
  AggregatingStateDescriptor,
  ValueState,
  ValueStateDescriptor
}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/** A stable bloom filter for deduplicating event streams.
  *
  * @see
  *   Approximately Detecting Duplicates for Streaming Data using Stable
  *   Bloom Filters by Fan Deng and Davood Rafiei, 2006.
  *   [[https://webdocs.cs.ualberta.ca/~drafiei/papers/DupDet06Sigmod.pdf]]
  *
  * @param sourceConfig
  *   The event source configuration. This can contain three properties to
  *   configure the stable bloom filter (all under <code>config.sbf</code>
  *   key:
  *   - <code>num.cells</code>: number of units of space (cells) to
  *     allocate (defaults to 1,000,000)
  *   - <code>bits.per.cell</code>: number of bits in each cell (defaults
  *     to 3)
  *   - <code>false.positive.rate</code>: rate of false positives to ensure
  *     (defaults to 0.001)
  *
  * @param identifier
  *   a function that creates a unique string from the incoming event to
  *   determine if it exists in the bloom filter (defaults to the event's
  *   <code>$id</code> member)
  * @tparam E
  *   the event stream type
  * @tparam ADT
  *   the flink runner algebraic data type
  */
class SBFDeduplicationFilter[E <: ADT: TypeInformation, ADT <: FlinkEvent](
    sourceConfig: SourceConfig[ADT],
    identifier: E => String = (e: E) => e.$dedupeId)
    extends RichFilterFunction[E]
    with LazyLogging {

  val numCells: Long            = sourceConfig.properties
    .getProperty("sbf.num.cells", "1000000")
    .toLong
  val bitsPerCell: Int          =
    sourceConfig.properties.getProperty("sbf.bits.per.cell", "3").toInt
  val falsePositiveRate: Double = sourceConfig.properties
    .getProperty("sbf.false.positive.rate", "0.01")
    .toDouble

  var bfs: AggregatingState[String, StableBloomFilter[CharSequence]] = _

  var dupCount: ValueState[Long] = _

  override def open(parameters: Configuration): Unit = {
    bfs = getRuntimeContext
      .getAggregatingState(
        new AggregatingStateDescriptor(
          s"${sourceConfig.label}_BloomFilter",
          new BloomFilterAggregateFunction(
            numCells,
            bitsPerCell,
            falsePositiveRate
          ),
          createTypeInformation[StableBloomFilter[CharSequence]]
        )
      )
    dupCount = getRuntimeContext.getState(
      new ValueStateDescriptor[Long](
        s"${sourceConfig.label}_DuplicateCount",
        createTypeInformation[Long]
      )
    )
  }

  override def filter(in: E): Boolean = {
    if (Option(dupCount.value()).isEmpty) dupCount.update(0L)

    // compute the identifier for the input event
    val id = identifier(in)

    // see if it's already in the bloom filter
    val alreadySeen = Option(bfs.get()).exists(_.mightContain(id))

    // if not, add it to the bloom filter
    if (!alreadySeen) bfs.add(id)
    else {
      // otherwise, record the duplicate
      val count = dupCount.value() + 1
      dupCount.update(count)
      logger.debug(
        s"${sourceConfig.name}: event $id already seen (dup count: $count)"
      )
    }

    // return our filter value, which means keep it wasn't in the bloom filter, or drop it otherwise
    !alreadySeen
  }

}
