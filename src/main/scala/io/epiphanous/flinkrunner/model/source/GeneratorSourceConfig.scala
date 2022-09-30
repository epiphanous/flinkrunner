package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.model._
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.{Source, SourceSplit}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.datagen.{
  DataGenerator,
  DataGeneratorSource
}

import java.time.Instant
import java.util.Random
import java.util.concurrent.atomic.AtomicLong

/** A source configuration for a Flink [[DataGeneratorSource]] function.
  *
  * This class is abstract because every generator is different. You must
  * implement a concrete subclass of this source config in order to use
  * this in your flink jobs.
  *
  * Configuration options:
  *   - `rows.per.second` - optional long defining number of rows per
  *     second to create (defaults to Long.MaxValue)
  *   - `max.rows` - optional max number of rows to produce (defaults to no
  *     max)
  *   - `seed` - optional random number generator seed (defaults to
  *     built-in java RNG value)
  *   - `start.ago` - optional duration from the current time into the past
  *     to start event timestamps (defaults to 1000 days ago)
  *   - `max.time.step.millis` - optional maximum number of millis to
  *     increment time by for each event (defaults to 100ms)
  *   - `prob.out.of.order` - optional probability of time moving backward
  *     (defaults to 0)
  *   - `prob.null` - optional probability of producing a null value
  *     (defaults to 0)
  * @tparam ADT
  *   Flinkrunner algebraic data type
  */
trait GeneratorSourceConfig[ADT <: FlinkEvent] extends SourceConfig[ADT] {

  override def connector: FlinkConnectorName = FlinkConnectorName.Generator

  val rowsPerSecond: Long    =
    config.getLongOpt("rows.per.second").getOrElse(Long.MaxValue)
  val maxRows: Long          = config.getLongOpt("max.rows").getOrElse(-1)
  val isBounded: Boolean     = Option(maxRows).nonEmpty
  val seedOpt: Option[Long]  = config.getLongOpt(pfx("seed"))
  val startTime: Instant     = Instant
    .now()
    .minusMillis(config.getDuration(pfx("start.ago")).toMillis)
  val maxTimeStep: Int       =
    config.getIntOpt(pfx("max.time.step.millis")).getOrElse(100)
  val probOutOfOrder: Double =
    config.getDoubleOpt(pfx("prob.out.of.order")).getOrElse(0)
  val probNull: Double       =
    config.getDoubleOpt(pfx("prob.null")).getOrElse(0)

  val rng: Random =
    seedOpt.map(s => new Random(s)).getOrElse(new Random())

  val timeSequence: AtomicLong = new AtomicLong(startTime.toEpochMilli)

  /** Return the current time sequence and move the time pointer. If the
    * optional parameter `byMillisOpt` is non-empty, the time pointer will
    * be incremented by that number of milliseconds. Otherwise, the time
    * pointer will be moved no more than the configured
    * `max.time.progression.millis` setting (`100ms` by default). The
    * direction of movement depends on the `prob.out.of.order` setting
    * (`0.1 percent`).
    * @param byMillisOpt
    *   optional millis to increment time by
    * @return
    *   current time as epoch millis
    */
  def getAndProgressTime(byMillisOpt: Option[Long] = None): Long = {
    val direction: Int =
      if (rng.nextGaussian() <= probOutOfOrder) {
        logger.trace("generating random value out of order")
        -1
      } else 1
    timeSequence.getAndAdd(
      byMillisOpt.getOrElse(
        direction * rng.nextInt(maxTimeStep * direction)
      )
    )
  }

  /** Returns true or false, according to the `prob.null` setting.
    * @return
    *   Boolean
    */
  def wantsNull: Boolean = rng.nextGaussian() <= probNull

  /** Returns a flink data generator. Must be implemented by subclasses.
    * @tparam E
    *   the type of event to generate (must be subclass of ADT)
    * @return
    *   DataGenerator[E]
    */
  def getGenerator[E <: ADT: TypeInformation]: DataGenerator[E] = ???

  /** Returns a flink data generator for an avro embedded event type. Must
    * be implemented by subclasses.
    *
    * @param fromKV
    *   an implicit function to generate an instance of E from an instance
    *   of A
    * @tparam E
    *   the type of event to generate (must be subclass of ADT)
    * @tparam A
    *   an avro type embedded in E
    * @return
    *   DataGenerator[E]
    */
  def getAvroGenerator[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](implicit
      fromKV: EmbeddedAvroRecordInfo[A] => E): DataGenerator[E] = ???

  override def getSource[E <: ADT: TypeInformation]
      : Either[SourceFunction[E], Source[E, _ <: SourceSplit, _]] =
    Left(
      new DataGeneratorSource(
        getGenerator[E],
        rowsPerSecond,
        if (maxRows > 0) maxRows else null
      )
    )

  override def getAvroSource[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](implicit
      fromKV: EmbeddedAvroRecordInfo[A] => E)
      : Either[SourceFunction[E], Source[E, _ <: SourceSplit, _]] =
    Left(
      new DataGeneratorSource(
        getAvroGenerator[E, A],
        rowsPerSecond,
        if (maxRows > 0) maxRows else null
      )
    )
}
