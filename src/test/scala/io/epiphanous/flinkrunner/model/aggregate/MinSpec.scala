package io.epiphanous.flinkrunner.model.aggregate

import io.epiphanous.flinkrunner.BasePropSpec
import io.epiphanous.flinkrunner.model.UnitMapper
import squants.Kilograms
import squants.mass.Mass

import java.time.Instant

class MinSpec extends BasePropSpec {
  property("updateQuantity property") {
    val m = Min(Mass.name, Kilograms.symbol)
    val t = Instant.now()
    val u = UnitMapper.defaultUnitMapper
    val q = for {
      m1 <- m.update(Kilograms(10), t, u)
      m2 <- m1.update(Kilograms(8), t, u)
    } yield m2.value
    q.value shouldBe (8)
  }
}
