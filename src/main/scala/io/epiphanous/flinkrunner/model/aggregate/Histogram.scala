package io.epiphanous.flinkrunner.model.aggregate

import io.epiphanous.flinkrunner.model.UnitMapper
import squants.Quantity

import java.time.Instant
import scala.util.Try

final case class Histogram(
    dimension: String,
    unit: String,
    value: Double = 0d,
    count: BigInt = BigInt(0),
    aggregatedLastUpdated: Instant = Instant.EPOCH,
    lastUpdated: Instant = Instant.now(),
    dependentAggregations: Map[String, Aggregate] =
      Map.empty[String, Aggregate],
    params: Map[String, String] = Map.empty[String, String])
    extends Aggregate {

  import Histogram._

  def bin(key: String): Aggregate =
    this.dependentAggregations.getOrElse(key, Count(dimension, unit))

  /** Compute a dynamic bin for the requested quantity. This picks a bin
    * based on the order of magnitude of the quantity in the aggregate's
    * preferred unit. If the order of magnitude is 3 (say the value is
    * 2345) For instance if the quantity value is 0.00157, its order of
    * magnitude is -3. We reduce that in absolute value by 1 (= -2) to
    * compute the min and max of the bin as [floor(0.0157 * 10**2)/10**2 (=
    * 0.01) and ceil(0.0157 * 10**2)/10**2 (= 0.02).
    *
    * @param q
    *   the quantity to compute a bin of
    * @return
    */
  def binOf[A <: Quantity[A]](
      q: A,
      unitMapper: UnitMapper): Try[String] = {
    unitMapper
      .createQuantity(q.dimension, value, unit)
      .map(_.unit)
      .map(u => (q in u).value)
      .map { d =>
        val absd         = math.abs(d)
        val magnitude    =
          math.floor(math.log10(if (absd < TOL) TOL else absd)).toInt
        val sign         = math.signum(magnitude)
        val abs          = math.abs(magnitude)
        val mag          = sign * (abs - 1)
        val pow          = math.pow(10, mag.toDouble)
        val min          = math.floor(d / pow) * pow
        val max          = math.ceil(d / pow) * pow
        val formatString = if (abs < 8) {
          val fs =
            s"%${if (sign < 0) "." else ""}$abs${if (sign > 0) ".0" else ""}"
          s"${fs}f,${fs}f"
        } else {
          "%e,%e"
        }
        formatString.format(min, max)
      }
  }

  override def update[A <: Quantity[A]](
      q: A,
      aggLU: Instant,
      unitMapper: UnitMapper): Try[Histogram] =
    binOf(q, unitMapper).flatMap { binKey =>
      bin(binKey).update(q.value, q.unit.symbol, aggLU, unitMapper).map {
        updatedBin =>
          copy(dependentAggregations =
            dependentAggregations.updated(binKey, updatedBin)
          )
      }
    }

  override def toString =
    s"$name[${this.dependentAggregations.map(kv => f"[${kv._1})=${kv._2.count}%d").mkString(", ")}"

}

object Histogram {
  final val TOL = 1e-9
}
