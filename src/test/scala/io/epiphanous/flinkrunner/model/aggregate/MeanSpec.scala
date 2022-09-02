package io.epiphanous.flinkrunner.model.aggregate

import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.model.UnitMapper
import squants.Kilograms
import squants.mass.Mass

import java.time.Instant
import scala.util.Success

class MeanSpec extends PropSpec {

  property("updateQuantity property") {
    val m = Mean(Mass.name, Kilograms.symbol)
    val t = Instant.now()
    val u = UnitMapper.defaultUnitMapper
    val q = for {
      m1 <- m.update(Kilograms(10), t, u)
      m2 <- m1.update(Kilograms(20), t, u)
      m3 <- m2.update(Kilograms(75), t, u)
    } yield m3.value
    q.get shouldEqual 35
    q.success shouldBe Success(35)
  }

}
