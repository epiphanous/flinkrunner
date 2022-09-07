package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.serde.DelimitedConfig
import org.apache.avro.generic.GenericRecord

import scala.collection.JavaConverters._
import scala.language.implicitConversions

sealed trait MyAvroADT extends FlinkEvent {
  def toJson(pretty: Boolean = false, sortKeys: Boolean = false): String
}

trait TestSerializers[A <: GenericRecord] {
  def $record: A
  def toJson(
      pretty: Boolean = false,
      sortKeys: Boolean = false): String = {
    val fields  = $record.getSchema.getFields.asScala.toList.map(_.name())
    val sfields = if (sortKeys) fields.sorted else fields
    sfields
      .map { name =>
        val value = $record.get(name) match {
          case None | null => "null"
          case s: String   => s""""${s.replaceAll("\"", "\\\n")}""""
          case value       => value.toString
        }
        s""""$name":${if (pretty) " " else ""}$value"""
      }
      .mkString(
        if (pretty) "{\n  " else "{",
        if (pretty) ",\n  " else ",",
        if (pretty) "\n}" else "}"
      )
  }

  def toDelimited(
      delimitedConfig: DelimitedConfig = DelimitedConfig.CSV): String =
    ""
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

object AWrapper extends EmbeddedAvroRecordFactory[AWrapper, ARecord] {
  override implicit def fromKV(
      info: EmbeddedAvroRecordInfo[ARecord]): AWrapper = AWrapper(
    info.record
  )
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

object BWrapper extends EmbeddedAvroRecordFactory[BWrapper, BRecord] {
  override implicit def fromKV(
      info: EmbeddedAvroRecordInfo[BRecord]): BWrapper = BWrapper(
    info.record
  )
}
