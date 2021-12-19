package io.epiphanous.flinkrunner.model.aggregate

import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.model.UnitMapper
import squants.Kilograms
import squants.mass.Mass

import java.time.Instant

class SumOfSquaredDeviationsSpec extends PropSpec {
  property("updateQuantity property") {
    val s = SumOfSquaredDeviations(Mass.name, Kilograms.symbol)
    val t = Instant.now()
    val u = UnitMapper.defaultUnitMapper
    val q = for {
      s1 <- s.update(Kilograms(10), t, u)
      s2 <- s1.update(Kilograms(20), t, u)
      s3 <- s2.update(Kilograms(30), t, u)
    } yield s3.value
    q.value shouldBe 200
  }
}
