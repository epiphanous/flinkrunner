package io.epiphanous.flinkrunner.model.aggregate

import io.epiphanous.flinkrunner.BasePropSpec
import squants.Kilograms
import squants.mass.Mass
import java.time.Instant

import io.epiphanous.flinkrunner.model.UnitMapper

class MaxSpec extends BasePropSpec {

  property("updateQuantity property") {
    val m = Max(Mass.name, Kilograms.symbol)
    val t = Instant.now()
    val u = UnitMapper.defaultUnitMapper
    val q = for {
      m1 <- m.update(Kilograms(10), t, u)
      m2 <- m1.update(Kilograms(16), t, u)
    } yield m2.value
    q.value shouldBe(16)
  }}
