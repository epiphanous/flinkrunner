package io.epiphanous.flinkrunner.model.Config
import enumeratum.EnumEntry.Hyphencase
import enumeratum._

sealed trait StateBackendEnum extends EnumEntry with Hyphencase
object StateBackendEnum extends Enum[StateBackendEnum] {
  val values = findValues
  case object RocksDB extends StateBackendEnum
  case object FileSystem extends StateBackendEnum
  case object Memory extends StateBackendEnum
}
