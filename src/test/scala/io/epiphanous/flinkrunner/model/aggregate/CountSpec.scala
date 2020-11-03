package io.epiphanous.flinkrunner.model.aggregate

import java.time.Instant

import io.epiphanous.flinkrunner.BasePropSpec
import io.epiphanous.flinkrunner.model.UnitMapper
import squants.Each

class CountSpec extends BasePropSpec {

  property("updateQuantity property") {
    val c = Count()
    val t = Instant.now()
    val u = UnitMapper.defaultUnitMapper
    val q = for {
      c1 <- c.update(Each(1),t,u)
      c2 <- c1.update(Each(1), t, u)
    } yield c2.value
    q.value shouldBe(2)
  }

}
