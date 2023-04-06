package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.serde.{DelimitedConfig, JsonConfig}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter
import org.apache.flink.table.types.logical.RowType

import scala.collection.JavaConverters._
import scala.language.implicitConversions

sealed trait MyAvroADT extends FlinkEvent {
  def toJson(
      jsonConfig: JsonConfig = JsonConfig(),
      record: Option[GenericRecord] = None): String
}

trait HasRowType {
  def getRowType: RowType
}

trait TestSerializers[A <: GenericRecord] {
  def $record: A

  def _serializeString(s: String): String =
    s""""${s.replaceAll("\"", "\\\n")}""""

  def toJson(
      jsonConfig: JsonConfig = JsonConfig(),
      record: Option[GenericRecord] = None): String = {
    val rec     = record.getOrElse($record)
    val fields  = rec.getSchema.getFields.asScala.toList
      .map(_.name())
    val sfields = if (jsonConfig.sortKeys) fields.sorted else fields
    sfields
      .map { name =>
        val value = rec.get(name) match {
          case None | null            => "null"
          case Some(s: String)        => _serializeString(s)
          case Some(r: GenericRecord) => toJson(jsonConfig, Some(r))
          case Some(v)                => v.toString
          case seq: Seq[_]            =>
            seq
              .map {
                case s: String        => _serializeString(s)
                case r: GenericRecord => toJson(jsonConfig, Some(r))
                case s                => s.toString
              }
              .mkString("[", ",", "]")
          case s: String              => _serializeString(s)
          case r: GenericRecord       => toJson(jsonConfig, Some(r))
          case v                      => v.toString
        }
        s""""$name":${if (jsonConfig.pretty) " " else ""}$value"""
      }
      .mkString(
        if (jsonConfig.pretty) "{\n  " else "{",
        if (jsonConfig.pretty) ",\n  " else ",",
        if (jsonConfig.pretty) "\n}" else "}"
      )
  }

  def toDelimited(
      delimitedConfig: DelimitedConfig = DelimitedConfig.CSV): String =
    ???
}

case class AWrapper(value: ARecord)
    extends MyAvroADT
    with EmbeddedAvroRecord[ARecord]
    with TestSerializers[ARecord] {
  override val $id: String      = value.a0
  override val $key: String     = $id
  override val $timestamp: Long = value.a3.toEpochMilli

  override val $recordKey: Option[String] = Some($id)
  override val $record: ARecord           = value
}

object AWrapper
    extends EmbeddedAvroRecordFactory[AWrapper, ARecord]
    with HasRowType {
  override implicit def fromKV(
      info: EmbeddedAvroRecordInfo[ARecord]): AWrapper = AWrapper(
    info.record
  )

  override def getRowType: RowType = AvroSchemaConverter
    .convertToDataType(ARecord.SCHEMA$.toString)
    .getLogicalType
    .asInstanceOf[RowType]
}

case class BWrapper(value: BRecord)
    extends MyAvroADT
    with EmbeddedAvroRecord[BRecord]
    with TestSerializers[BRecord] {
  override val $id: String                = value.b0
  override val $key: String               = $id
  override val $timestamp: Long           = value.b3.toEpochMilli
  override val $recordKey: Option[String] = Some($id)
  override val $record: BRecord           = value
}

object BWrapper
    extends EmbeddedAvroRecordFactory[BWrapper, BRecord]
    with HasRowType {
  override implicit def fromKV(
      info: EmbeddedAvroRecordInfo[BRecord]): BWrapper = BWrapper(
    info.record
  )

  override def getRowType: RowType = AvroSchemaConverter
    .convertToDataType(BRecord.SCHEMA$.toString)
    .getLogicalType
    .asInstanceOf[RowType]

}

case class CWrapper(value: CRecord)
    extends MyAvroADT
    with EmbeddedAvroRecord[CRecord]
    with TestSerializers[CRecord] {
  override def $record: CRecord = value

  override def $id: String = value.id

  override def $key: String = value.id

  override def $recordKey: Option[String] = Some($key)

  override def $timestamp: Long = value.ts.toEpochMilli
}

object CWrapper
    extends EmbeddedAvroRecordFactory[CWrapper, CRecord]
    with HasRowType {
  override implicit def fromKV(
      info: EmbeddedAvroRecordInfo[CRecord]): CWrapper = CWrapper(
    info.record
  )

  override def getRowType: RowType = AvroSchemaConverter
    .convertToDataType(CRecord.SCHEMA$.toString)
    .getLogicalType
    .asInstanceOf[RowType]

}
