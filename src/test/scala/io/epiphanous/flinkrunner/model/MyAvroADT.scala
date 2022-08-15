package io.epiphanous.flinkrunner.model

import scala.language.implicitConversions

sealed trait MyAvroADT extends FlinkEvent

case class AWrapper(value: ARecord)
    extends MyAvroADT
    with EmbeddedAvroRecord[ARecord] {
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
    with EmbeddedAvroRecord[BRecord] {
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
