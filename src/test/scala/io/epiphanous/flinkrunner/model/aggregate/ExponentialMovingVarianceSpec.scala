package io.epiphanous.flinkrunner.model.aggregate

import io.epiphanous.flinkrunner.BasePropSpec
import io.epiphanous.flinkrunner.model.UnitMapper
import org.scalactic.{Equality, TolerantNumerics}
import squants.Kilograms
import squants.mass.Mass

import java.time.Instant

class ExponentialMovingVarianceSpec extends BasePropSpec {

  implicit val tol: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-4)

  property("updateQuantity property") {
    val v = ExponentialMovingVariance(Mass.name, Kilograms.symbol)
    val t = Instant.now()
    val u = UnitMapper.defaultUnitMapper
    val q = for {
      v1 <- v.update(Kilograms(10), t, u)
      v2 <- v1.update(Kilograms(20), t, u)
      v3 <- v2.update(Kilograms(30), t, u)
    } yield v3.value
    q.value shouldEqual (41.79)
  }
}
