package io.epiphanous.flinkrunner.model.aggregate

import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.model.UnitMapper
import squants.Each

import java.time.Instant
import scala.util.Success

class CountSpec extends PropSpec {

  property("updateQuantity property") {
    val c = Count()
    val t = Instant.now()
    val u = UnitMapper.defaultUnitMapper
    val q = for {
      c1 <- c.update(Each(1), t, u)
      c2 <- c1.update(Each(1), t, u)
    } yield c2.value
    q.success shouldBe Success(2)
  }

}
