package io.epiphanous.flinkrunner.model.aggregate

import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.model.UnitMapper
import squants.Kilograms
import squants.mass.Mass

import java.time.Instant
import scala.util.Success

class RangeSpec extends PropSpec {

  property("updateQuantity property") {
    val q = for {
      r <- Success(Range(Mass.name, Kilograms.symbol))
      r2 <- r.update(
              Kilograms(10),
              Instant.now(),
              UnitMapper.defaultUnitMapper
            )
      r3 <- r2.update(
              Kilograms(30),
              Instant.now(),
              UnitMapper.defaultUnitMapper
            )
      r4 <- r3.update(
              Kilograms(37),
              Instant.now(),
              UnitMapper.defaultUnitMapper
            )
    } yield r4.labeledValue
    q.success shouldBe Success("Range: 27.000000 kg")
  }

}
