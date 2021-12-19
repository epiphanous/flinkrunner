package io.epiphanous.flinkrunner.model.aggregate

import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.model.UnitMapper
import squants.Kilograms
import squants.mass.Mass

import java.time.Instant

class ExponentialMovingAverageSpec extends PropSpec {
  property("updateQuantity property") {
    val a = ExponentialMovingAverage(Mass.name, Kilograms.symbol)
    val t = Instant.now()
    val u = UnitMapper.defaultUnitMapper
    val q = for {
      a1 <- a.update(Kilograms(10), t, u)
      a2 <- a1.update(Kilograms(20), t, u)
      a3 <- a2.update(Kilograms(30), t, u)
    } yield a3.value
    q.value shouldBe 26.1
  }
}
