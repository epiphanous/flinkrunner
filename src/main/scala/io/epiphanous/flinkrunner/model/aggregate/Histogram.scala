package io.epiphanous.flinkrunner.model.aggregate
import java.time.Instant

import squants.Quantity

final case class Histogram(
  dimension: String,
  unit: String,
  value: Double = 0d,
  name: String = "Histogram",
  count: BigInt = BigInt(0),
  aggregatedLastUpdated: Instant = Instant.EPOCH,
  lastUpdated: Instant = Instant.now(),
  dependentAggregations: Map[String, Aggregate] = Map.empty[String, Aggregate],
  params: Map[String, Any] = Map.empty[String, Any])
    extends Aggregate {

  import Histogram._

  def withDependents(depAggs: Map[String, Aggregate]) = copy(dependentAggregations = depAggs)

  def bin(key: String): Aggregate = this.dependentAggregations.getOrElse(key, Count())

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
  def binOf[A <: Quantity[A]](q: A) = {
    q.dimension
      .symbolToUnit(unit)
      .map(u => (q in u).value)
      .map(d => {
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
      })
  }

  override def update[A <: Quantity[A]](q: A, aggLU: Instant) =
    binOf(q) match {
      case Some(binKey) =>
        bin(binKey)
          .update(q.value, q.unit.symbol, aggLU) match {
          case Some(updatedBin) => Some(copy(dependentAggregations = dependentAggregations.updated(binKey, updatedBin)))
          case None => {
            logger.error(s"$name[$dimension,$unit] Quantity[$q] can't be binned")
            None
          }
        }
      case None =>
        logger.error(s"$name[$dimension,$unit] can't be updated with $q")
        None
    }

  override def toString =
    s"$name[${this.dependentAggregations.map(kv => f"${kv._1}=${kv._2.value}%.0f").mkString(", ")}"

}

object Histogram {
  final val LN10 = math.log(10)
  final val TOL = 1e-9
}
