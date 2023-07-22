package io.epiphanous.flinkrunner.model

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.flink.table.annotation.DataTypeHint
import org.apache.flink.table.data.RowData
import org.apache.iceberg.Schema
import org.apache.iceberg.data.{GenericRecord, Record}
import org.apache.iceberg.types.Types

import java.time.{Instant, LocalDateTime, OffsetDateTime, ZoneOffset}
import scala.language.implicitConversions

trait ToIceberg {
  def toIcebergRecord: GenericRecord
}

trait FromIcebergRecord[E <: FlinkEvent] {
  def ICEBERG_SCHEMA: Schema
  implicit def fromIcebergRecord(r: Record): E
}

sealed trait MySimpleADT
    extends FlinkEvent
    with EmbeddedRowType
    with ToIceberg

case class SimpleA(id: String, a0: String, a1: Int, ts: Instant)
    extends MySimpleADT
    with EmbeddedRowType
    with Ordered[SimpleA] {
  override def $id: String = id

  override def $key: String = a0

  override def $timestamp: Long = ts.toEpochMilli

  override def toIcebergRecord: GenericRecord = {
    val record = GenericRecord.create(SimpleA.ICEBERG_SCHEMA)
    record.setField("id", id)
    record.setField("a0", a0)
    record.setField("a1", a1)
    record.setField("ts", ts)
    record
  }

  override def compare(that: SimpleA): Int = this.id.compare(that.id)
}

object SimpleA
    extends EmbeddedRowTypeFactory[SimpleA]
    with FromIcebergRecord[SimpleA] {
  override implicit def fromRowData(rowData: RowData): SimpleA = SimpleA(
    rowData.getString(0).toString,
    rowData.getString(1).toString,
    rowData.getInt(2),
    rowData.getTimestamp(3, 3).toInstant
  )

  override val ICEBERG_SCHEMA: Schema = new Schema(
    Types.NestedField.required(1, "id", Types.StringType.get()),
    Types.NestedField.required(2, "a0", Types.StringType.get()),
    Types.NestedField.required(3, "a1", Types.IntegerType.get()),
    Types.NestedField.required(4, "ts", Types.TimestampType.withoutZone())
  )

  override implicit def fromIcebergRecord(r: Record): SimpleA = ???
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

  override def toIcebergRecord: GenericRecord = {
    val record = GenericRecord.create(SimpleB.ICEBERG_SCHEMA)
    record.setField("id", id)
    record.setField("b0", b0)
    record.setField("b1", b1)
    record.setField("b2", b2.orNull)
    record.setField("ts", LocalDateTime.ofInstant(ts, ZoneOffset.UTC))
    record
  }
}

object SimpleB
    extends EmbeddedRowTypeFactory[SimpleB]
    with FromIcebergRecord[SimpleB] {

  override implicit def fromRowData(rowData: RowData): SimpleB = {
    SimpleB(
      rowData.getString(0).toString,
      rowData.getString(1).toString,
      rowData.getDouble(2),
      if (rowData.isNullAt(3)) None else Some(rowData.getInt(3)),
      rowData.getTimestamp(4, 3).toInstant
    )
  }

  implicit def fromIcebergRecord(record: Record): SimpleB =
    SimpleB(
      record.getField("id").asInstanceOf[String],
      record.getField("b0").asInstanceOf[String],
      record.getField("b1").asInstanceOf[Double],
      Option(record.getField("b2")).map(_.asInstanceOf[Int]),
      record.getField("ts").asInstanceOf[OffsetDateTime].toInstant
    )

  override val ICEBERG_SCHEMA: Schema = new Schema(
    Types.NestedField.required(1, "id", Types.StringType.get()),
    Types.NestedField.required(2, "b0", Types.StringType.get()),
    Types.NestedField.required(3, "b1", Types.DoubleType.get()),
    Types.NestedField.optional(4, "b2", Types.IntegerType.get()),
    Types.NestedField.required(5, "ts", Types.TimestampType.withoutZone())
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

  override def toIcebergRecord: GenericRecord = ???
}

object SimpleC
    extends EmbeddedRowTypeFactory[SimpleC]
    with FromIcebergRecord[SimpleC] {
  override implicit def fromRowData(rowData: RowData): SimpleC = SimpleC(
    rowData.getString(0).toString,
    rowData.getString(1).toString,
    rowData.getDouble(2),
    rowData.getInt(3),
    rowData.getTimestamp(4, 3).toInstant
  )

  override val ICEBERG_SCHEMA: Schema = new Schema(
    Types.NestedField.required(1, "id", Types.StringType.get()),
    Types.NestedField.required(2, "c1", Types.StringType.get()),
    Types.NestedField.required(3, "c2", Types.DoubleType.get()),
    Types.NestedField.required(4, "c3", Types.IntegerType.get()),
    Types.NestedField.required(5, "ts", Types.TimestampType.withoutZone())
  )

  override implicit def fromIcebergRecord(r: Record): SimpleC = ???
}
