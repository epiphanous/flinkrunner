package io.epiphanous.flinkrunner.algorithm.membership

import com.google.common.hash.Funnels
import io.epiphanous.flinkrunner.UnitSpec

import java.nio.charset.StandardCharsets

class StableBloomFilterTest extends UnitSpec {

  val bfBuilder: StableBloomFilterBuilder[CharSequence] =
    StableBloomFilterBuilder(
      Funnels.stringFunnel(StandardCharsets.UTF_8)
    )

  def bstr(x: Long): String = {
    val s  = x.toBinaryString
    val s1 = "0" * (63 - s.length) + s
    s1.grouped(3).mkString("|")
  }

  behavior of "StableBloomFilterTest"

  it should "add" in {
    val bf    = bfBuilder
      .withNumCells(21)
      .build()
    val items = List("first item", "second item")
    println(items.map(bf.hash))
    val a     = bf.add(items.head)
    println(bstr(bf.storage(0)))
    val b     = bf.add(items(1))
    println(bstr(bf.storage(0)))
    val c     = bf.add(items.head)
    println(bstr(bf.storage(0)))
    a shouldBe false
    b shouldBe false
    c shouldBe true
  }

  it should "offset" in {
    val bf = bfBuilder.build()
//    Range(0, 100).foreach(i => println((i, bf.offset(i.toLong))))
    bf.offset(21L) shouldEqual (1, 0)
    bf.offset(41L) shouldEqual (1, 60)
  }

  /** found this test on
    * https://www.waitingforcode.com/big-data-algorithms/stable-bloom-filter/read
    */
  it should "not find false positives with low false positives rate" in {
    val bf = bfBuilder
      .withBitsPerCell(8)
      .build()

    val N = 10000

    val data = (0 until N)
      .map(_.toString)
      .map { n =>
        bf.add(n)
        n
      }
      .toSet

    val falseNegatives = data.filter(!bf.mightContain(_))
    falseNegatives shouldBe empty

    val falsePositives = ((N + 1) until (5 * N - 1))
      .map(_.toString)
      .filter(n => bf.mightContain(n))
    falsePositives shouldBe empty

    bf.Max shouldEqual 255
    bf.K shouldEqual 123
    bf.P shouldEqual 9433
  }

  /** found this test on
    * https://www.waitingforcode.com/big-data-algorithms/stable-bloom-filter/read
    */
  it should "perform worse with smaller filter" in {
    val bf = bfBuilder
      .withBitsPerCell(3)
      .withNumCells(1000L)
      .build()

    val N = 10000

    val data = (0 until N)
      .map(_.toString)
      .map { n =>
        bf.add(n)
        n
      }
      .toSet

    val falseNegatives = data.filter(!bf.mightContain(_))
    println(s"number of false negatives = ${falseNegatives.size}")
    falseNegatives.size should be > N / 2

    val falsePositives = ((N + 1) until (5 * N - 1))
      .map(_.toString)
      .filter(n => bf.mightContain(n))
    println(s"Number of false positives = ${falsePositives.size}")
    falsePositives.size should be > N / 100

    bf.Max shouldEqual 7
    bf.K shouldEqual 4
    bf.P shouldEqual 71
  }

}
