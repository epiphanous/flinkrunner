package io.epiphanous.flinkrunner.model

import java.nio.ByteBuffer
import scala.annotation.switch
import scala.collection.mutable

/** A copy-pastable, url friendly, ascii embeddable, lexicographically
  * sortable binary encoding
  *
  * Adapted from javascript code at https://github.com/dominictarr/d64
  */
object D64 {

  final val chars =
    ".PYFGCRLAOEUIDHTNSQJKXBMWVZ_pyfgcrlaoeuidhtnsqjkxbmwvz1234567890"
      .split("")
      .sorted
      .mkString("")

  final val codeToIndex = Array.fill[Int](128)(0)
  chars.zipWithIndex.foreach { case (c, i) => codeToIndex(c.toInt) = i }

  /** encode binary data to string */
  def encode(data: Array[Byte]): String = {
    val sb   = new mutable.StringBuilder("")
    val len  = data.length
    var hang = 0
    data.zipWithIndex.foreach { case (v, i) =>
      val v2 = if (v < 0) v.toInt + 0x100 else v.toInt
      (i % 3: @switch) match {
        case 0 =>
          sb += chars(v2 >> 2)
          hang = (v2 & 3) << 4
        case 1 =>
          sb += chars(hang | v2 >> 4)
          hang = (v2 & 0xf) << 2
        case 2 =>
          sb += chars(hang | v2 >> 6)
          sb += chars(v2 & 0x3f)
          hang = 0
      }
    }
    if ((len % 3) > 0) sb += chars(hang)
    sb.mkString
  }

  /** decode an encoded string back to binary data */
  def decode(str: String): Array[Byte] = {
    val len   = str.length
    val bytes = ByteBuffer.allocate(Math.floor((len / 4d) * 3d).toInt)
    var hang  = 0
    var j     = 0
    str.zipWithIndex.foreach { case (_, i) =>
      val v = codeToIndex(str.codePointAt(i))
      (i % 4: @switch) match {
        case 0 => hang = v << 2
        case 1 =>
          bytes.put(j, (hang | v >> 4).toByte)
          j += 1
          hang = (v << 4) & 0xff
        case 2 =>
          bytes.put(j, (hang | v >> 2).toByte)
          j += 1
          hang = (v << 6) & 0xff
        case 3 =>
          bytes.put(j, (hang | v).toByte)
          j += 1
      }
    }
    bytes.array()
  }

}
