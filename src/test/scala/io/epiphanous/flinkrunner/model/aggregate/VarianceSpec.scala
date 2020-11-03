package io.epiphanous.flinkrunner.model.aggregate

import java.time.Instant

import io.epiphanous.flinkrunner.BasePropSpec
import io.epiphanous.flinkrunner.model.UnitMapper
import squants.Kilograms
import squants.mass.Mass

class VarianceSpec extends BasePropSpec {

  property("updateQuantity property") {
    val v = Variance(Mass.name, Kilograms.symbol)
    val t = Instant.now()
    val u = UnitMapper.defaultUnitMapper
    val q = for {
      v1 <- v.update(Kilograms(10), t, u)
      v2 <- v1.update(Kilograms(20), t, u)
      v3 <- v2.update(Kilograms(30), t, u)
      v4 <- v3.update(Kilograms(40), t, u)
    } yield v4.value
    q.value shouldBe(166 + 2d/3)
  }

}
