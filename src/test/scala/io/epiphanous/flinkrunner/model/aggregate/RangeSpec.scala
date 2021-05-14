package io.epiphanous.flinkrunner.model.aggregate

import io.epiphanous.flinkrunner.BasePropSpec
import io.epiphanous.flinkrunner.model.UnitMapper
import squants.Kilograms
import squants.mass.Mass

import java.time.Instant

class RangeSpec extends BasePropSpec {

  property("updateQuantity property") {
    val q = for {
      r <- Some(Range(Mass.name, Kilograms.symbol))
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
    q.value shouldBe "Range: 27.000000 kg"
  }

}
