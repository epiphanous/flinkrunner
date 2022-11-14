package io.epiphanous.flinkrunner.model

import com.fasterxml.jackson.databind.annotation.JsonDeserialize

import java.time.Instant

sealed trait MySimpleADT extends FlinkEvent

case class SimpleA(id: String, a0: String, a1: Int, ts: Instant)
    extends MySimpleADT {
  override def $id: String = id

  override def $key: String = a0

  override def $timestamp: Long = ts.toEpochMilli
}

/** Simple Class. Note this has a field (b2) that is an Option[Int]. If we
  * plan on deserializing this with Jackson (ie, with our json or csv
  * decoding classes), such wrapped types must be annotated with
  * `@JsonDeserialize(contentAs = classOf[java internal type])`.
  *
  * @see
  *   [[https://github.com/FasterXML/jackson-module-scala/wiki/FAQ#deserializing-optionint-and-other-primitive-challenges]].
  *
  * @param id
  *   a String
  * @param b0
  *   a String
  * @param b1
  *   a Double
  * @param b2
  *   an Option[Int]
  * @param ts
  *   an Instant
  */
case class SimpleB(
    id: String,
    b0: String,
    b1: Double,
    @JsonDeserialize(contentAs = classOf[java.lang.Integer]) b2: Option[
      Int
    ],
    ts: Instant)
    extends MySimpleADT {
  override def $id: String = id

  override def $key: String = b0

  override def $timestamp: Long = ts.toEpochMilli
}

case class SimpleC(
    id: String,
    c1: String,
    c2: Double,
    c3: Int,
    ts: Instant)
    extends MySimpleADT {
  override def $id: String = id

  override def $key: String = c1

  override def $timestamp: Long = ts.toEpochMilli
}
