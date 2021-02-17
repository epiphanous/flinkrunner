package io.epiphanous.flinkrunner.algorithm.membership

import com.google.common.hash.Funnels
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets

class StableBloomFilterTest extends AnyFlatSpec with Matchers {

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
