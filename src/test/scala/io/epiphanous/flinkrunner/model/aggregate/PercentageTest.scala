package io.epiphanous.flinkrunner.model.aggregate

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import squants.time.{Seconds, Time}

import java.time.Instant
import scala.util.Success

class PercentageTest extends AnyFlatSpec with Matchers with LazyLogging {
  behavior of "PercentageTest"

  val p = Percentage(Time.name, Seconds.symbol, 86400d)

  it should "update" in {
    val q = p.update(86400 / 2, "s", Instant.now())
    q.map(_.value) shouldBe Success(50)
  }

  it should "dimension" in {
    p.dimension shouldBe "Time"
  }

  it should "value" in {
    p.value shouldBe 0d
  }

  it should "baseParam" in {
    p.baseParam shouldEqual 86400d
  }

  it should "params" in {
    p.params.isEmpty shouldBe false
    p.params.contains("base") shouldBe true
    p.params.get("base") shouldBe Some("86400.0")
  }

  it should "name" in {
    p.name shouldBe "Percentage"
  }

}
