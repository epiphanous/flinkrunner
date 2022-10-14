package io.epiphanous.flinkrunner.model

import java.time.{Duration, Instant}
import java.util.concurrent.atomic.AtomicLong
import java.util.{Properties, Random}
import scala.collection.JavaConverters._

/** Configuration for a data generator.
  * @param rowsPerSecond
  *   number of rows that should be generated per second (defaults to max
  *   speed)
  * @param maxRows
  *   the maximum number of rows to generate (defaults to -1, meaning no
  *   limit)
  * @param seedOpt
  *   an optional seed for the random number generator (defaults to None)
  * @param startTime
  *   the start time of the generator (defaults to now)
  * @param maxTimeStep
  *   the maximum time step (in millis) after each event is generated
  *   (defaults to 100ms)
  * @param probOutOfOrder
  *   the probability of moving backwards in time before the next event is
  *   generated (defaults to zero)
  * @param probNull
  *   the probability of generating a null value
  * @param properties
  *   custom properties for implementing generators
  */
case class GeneratorConfig(
    name: String,
    rowsPerSecond: Long = Long.MaxValue,
    maxRows: Long = -1L,
    seedOpt: Option[Long] = None,
    startAgo: Duration = Duration.ofDays(365),
    maxTimeStep: Int = 100,
    probOutOfOrder: Double = 0,
    probNull: Double = 0,
    properties: Properties = new Properties()) {

  /** A random number generator */
  val rng: Random = seedOpt.map(s => new Random(s)).getOrElse(new Random())

  /** The start time of the generator's time sequence */
  val startTime: Instant = Instant.now().minusMillis(startAgo.toMillis)

  /** A time sequence to simulate the movement of time as we generate
    * events
    */
  val timeSequence: AtomicLong = new AtomicLong(startTime.toEpochMilli)

  /** Return the current time sequence and move the time pointer. If the
    * optional parameter `byMillisOpt` is non-empty, the time pointer will
    * be incremented by that number of milliseconds. Otherwise, the time
    * pointer will be moved no more than the configured
    * `max.time.progression.millis` setting (`100ms` by default). The
    * direction of movement depends on the `prob.out.of.order` setting
    * (`0.1 percent`).
    *
    * @param byMillisOpt
    *   optional millis to increment time by
    * @return
    *   current time as epoch millis
    */
  def getAndProgressTime(byMillisOpt: Option[Long] = None): Long =
    timeSequence.getAndAdd(byMillisOpt.getOrElse {
      val direction: Int =
        if (rng.nextDouble() <= probOutOfOrder) -1
        else 1
      val increment      = direction * rng.nextInt(maxTimeStep)
      increment.toLong
    })

  /** Return true if next value generated should be null
    * @return
    *   Boolean
    */
  def wantsNull: Boolean = rng.nextDouble() <= probNull

  /** Get a property from our custom properties, or return a default.
    * @param prop
    *   the property name
    * @param alt
    *   the default value
    * @tparam T
    *   the type of the property
    * @return
    *   an instance of T
    */
  def getProp[T](prop: String, alt: T): T = {
    val value = Option(properties.getProperty(prop))
    (alt match {
      case int: Int         => value.map(_.toInt).getOrElse(int)
      case string: String   => value.getOrElse(string)
      case double: Double   => value.map(_.toDouble).getOrElse(double)
      case long: Long       => value.map(_.toLong).getOrElse(long)
      case boolean: Boolean => value.map(_.toBoolean).getOrElse(boolean)
      case _                =>
        throw new RuntimeException(
          s"unknown type ${alt.getClass.getCanonicalName} for property $prop in generator $name"
        )
    }).asInstanceOf[T]
  }

  def getRandInt: Int                            = rng.nextInt()
  def getRandInt(bound: Int): Int                = rng.nextInt(bound)
  def getRandLong: Long                          = rng.nextLong()
  def getRandDouble: Double                      = rng.nextDouble()
  def getRandBoolean: Boolean                    = rng.nextBoolean()
  def getRandString(maxLength: Int = 20): String = {
    rng
      .ints(48, 123)
      .filter(i => (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
      .limit(maxLength.toLong)
      .iterator()
      .asScala
      .map(_.toChar)
      .mkString("")
  }
}
