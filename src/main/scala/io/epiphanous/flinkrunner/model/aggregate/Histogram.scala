package io.epiphanous.flinkrunner.model.aggregate
import java.time.Instant

import squants.{Quantity, UnitOfMeasure}

final case class Histogram[A <: Quantity[A]](
  unit: UnitOfMeasure[A],
  value: Option[A] = None,
  count: BigInt = BigInt(0),
  name: String = "Count",
  aggregatedLastUpdated: Instant = Instant.now(),
  lastUpdated: Instant = Instant.now(),
  bins: Map[String, Count[A]] = Map.empty[String, Count[A]])
    extends Aggregate[A] {

  import Histogram._

  def bin(key: String): Count[A] = bins.getOrElse(key, Count(unit))

  /** Compute a dynamic bin for the requested quantity. This picks a bin
    * based on the order of magnitude of the quantity in the aggregate's preferred unit.
    * If the order of magnitude is 3 (say the value is 2345)
    * For instance if the quantity value is 0.00157, its order of magnitude is -3. We
    * reduce that in absolute value by 1 (= -2) to compute the min and max of the bin as
    * [floor(0.0157 * 10**2)/10**2 (= 0.01) and
    * ceil(0.0157 * 10**2)/10**2 (= 0.02).
    *
    * @param q the quantity to compute a bin of
    * @return
    */
  def binOf(q: A) = {
    val d = (q in unit).value
    val magnitude =
      math.floor(math.log(math.abs(d)) / LN10 + TOL).toInt
    val sign = math.signum(magnitude)
    val abs = math.abs(magnitude)
    val mag = sign * (abs - 1)
    val pow = math.pow(10, mag)
    val min = math.floor(d / pow) * pow
    val max = math.ceil(d / pow) * pow
    val formatString = if (abs < 8) {
      val fs = s"%${if (sign < 0) "." else ""}$abs${if (sign > 0) ".0" else ""}"
      s"${fs}f,${fs}f"
    } else {
      "%e,%e"
    }
    formatString.format(min, max)
  }

  override def update(q: A, aggLastUpdated: Instant) = {
    val binKey = binOf(q)
    val updatedBin = bin(binKey).update(q, aggLastUpdated)
    copy(value = Some(q),
         count = count + 1,
         aggregatedLastUpdated = aggLastUpdated,
         bins = bins.updated(binKey, updatedBin))
  }
}

object Histogram {
  final val LN10 = math.log(10)
  final val TOL = 1e-9
}
