package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.PropSpec

import java.nio.ByteBuffer
import scala.collection.mutable
import scala.util.Random

class D64Spec extends PropSpec {

  val rng = new Random()

  def toHex(bytes: Array[Byte]): String = {
    val sb = new mutable.StringBuilder("")
    bytes.foreach(b => sb.append(String.format("%02x", Byte.box(b))))
    sb.toString()
  }

  property("round trip") {
    val bytes = ByteBuffer.allocate(36).array()
    Range(0, 100).foreach { _ =>
      rng.nextBytes(bytes)
      val code   = D64.encode(bytes)
//      println(toHex(bytes), code)
      val actual = D64.decode(code)
//      println(toHex(actual))
      actual shouldEqual bytes
    }

  }
}
