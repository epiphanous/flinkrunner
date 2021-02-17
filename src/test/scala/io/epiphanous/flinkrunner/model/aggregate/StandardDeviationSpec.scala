package io.epiphanous.flinkrunner.model.aggregate

import io.epiphanous.flinkrunner.BasePropSpec
import io.epiphanous.flinkrunner.model.UnitMapper
import squants.Kilograms
import squants.mass.Mass

import java.time.Instant

class StandardDeviationSpec extends BasePropSpec {

  property("updateQuantity property") {
    val s = StandardDeviation(Mass.name, Kilograms.symbol)
    val t = Instant.now()
    val u = UnitMapper.defaultUnitMapper
    val q = for {
      s1 <- s.update(Kilograms(10), t, u)
      s2 <- s1.update(Kilograms(20), t, u)
      s3 <- s2.update(Kilograms(30), t, u)
      s4 <- s3.update(Kilograms(40), t, u)
    } yield s4.value
    q.value shouldBe (Math.sqrt(166 + 2d / 3))
  }

}
