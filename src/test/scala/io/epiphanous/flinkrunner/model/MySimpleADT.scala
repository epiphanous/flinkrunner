package io.epiphanous.flinkrunner.model

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.flink.table.annotation.DataTypeHint
import org.apache.flink.table.data.RowData

import java.time.Instant
import scala.language.implicitConversions

sealed trait MySimpleADT extends FlinkEvent with EmbeddedRowType

case class SimpleA(id: String, a0: String, a1: Int, ts: Instant)
    extends MySimpleADT
    with EmbeddedRowType {
  override def $id: String = id

  override def $key: String = a0

  override def $timestamp: Long = ts.toEpochMilli
}

object SimpleA extends EmbeddedRowTypeFactory[SimpleA] {
  override implicit def fromRowData(rowData: RowData): SimpleA = SimpleA(
    rowData.getString(0).toString,
    rowData.getString(1).toString,
    rowData.getInt(2),
    rowData.getTimestamp(3, 3).toInstant
  )

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
    @JsonDeserialize(contentAs = classOf[java.lang.Integer])
    @DataTypeHint("INT")
    b2: Option[
      Int
    ],
    ts: Instant)
    extends MySimpleADT
    with EmbeddedRowType {
  override def $id: String = id

  override def $key: String = b0

  override def $timestamp: Long = ts.toEpochMilli
}

object SimpleB extends EmbeddedRowTypeFactory[SimpleB] {

  override implicit def fromRowData(rowData: RowData): SimpleB = SimpleB(
    rowData.getString(0).toString,
    rowData.getString(1).toString,
    rowData.getDouble(2),
    Option(rowData.getInt(3)),
    rowData.getTimestamp(4, 3).toInstant
  )
}

case class SimpleC(
    id: String,
    c1: String,
    c2: Double,
    c3: Int,
    ts: Instant)
    extends MySimpleADT
    with EmbeddedRowType {
  override def $id: String = id

  override def $key: String = c1

  override def $timestamp: Long = ts.toEpochMilli

}

object SimpleC extends EmbeddedRowTypeFactory[SimpleC] {
  override implicit def fromRowData(rowData: RowData): SimpleC = SimpleC(
    rowData.getString(0).toString,
    rowData.getString(1).toString,
    rowData.getDouble(2),
    rowData.getInt(3),
    rowData.getTimestamp(4, 3).toInstant
  )

}
