package io.epiphanous.flinkrunner.algorithm.membership
import java.nio.charset.StandardCharsets

import com.google.common.hash.Funnels
import org.scalatest.{FlatSpec, Matchers}

class StableBloomFilterTest extends FlatSpec with Matchers {

  val bf = StableBloomFilterBuilder(Funnels.stringFunnel(StandardCharsets.UTF_8)).build()

  behavior of "StableBloomFilterTest"

  it should "add" in {
    val a = bf.add("first item")
    val b = bf.add("another item")
    val c = bf.add("first item")
    a shouldBe false
    b shouldBe false
    c shouldBe true
  }

}
