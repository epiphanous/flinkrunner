package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.serde.DelimitedConfig
import org.apache.avro.generic.GenericRecord

import scala.collection.JavaConverters._
import scala.language.implicitConversions

sealed trait MyAvroADT extends FlinkEvent {
  def toJson(
      pretty: Boolean = false,
      sortKeys: Boolean = false,
      record: Option[GenericRecord] = None): String
}

trait TestSerializers[A <: GenericRecord] {
  def $record: A

  def _serializeString(s: String): String =
    s""""${s.replaceAll("\"", "\\\n")}""""

  def toJson(
      pretty: Boolean = false,
      sortKeys: Boolean = false,
      record: Option[GenericRecord] = None): String = {
    val rec     = record.getOrElse($record)
    val fields  = rec.getSchema.getFields.asScala.toList
      .map(_.name())
    val sfields = if (sortKeys) fields.sorted else fields
    sfields
      .map { name =>
        val value = rec.get(name) match {
          case None | null             => "null"
          case Some(s: String)         => _serializeString(s)
          case Some(r: GenericRecord)  => toJson(pretty, sortKeys, Some(r))
          case Some(v)                 => v.toString
          case seq: Seq[String]        =>
            seq.map(_serializeString).mkString("[", ",", "]")
          case seq: Seq[GenericRecord] =>
            seq
              .map(r => toJson(pretty, sortKeys, Some(r)))
              .mkString("[", ",", "]")
          case seq: Seq[_]             => seq.map(_.toString).mkString("[", ",", "]")
          case s: String               => _serializeString(s)
          case r: GenericRecord        => toJson(pretty, sortKeys, Some(r))
          case v                       => v.toString
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

object CWrapper extends EmbeddedAvroRecordFactory[CWrapper, CRecord] {
  override implicit def fromKV(
      info: EmbeddedAvroRecordInfo[CRecord]): CWrapper = CWrapper(
    info.record
  )
}
