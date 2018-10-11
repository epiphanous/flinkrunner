package io.epiphanous.flinkrunner.model
import java.util.UUID

case class DataControlPeriod[D <: FlinkEvent](
    id: String = UUID.randomUUID().toString,
    key: String,
    start: Long = 0L,
    end: Long = 0L,
    elements: List[D] = List.empty)
    extends FlinkEvent {
  def duration: Long            = end - start
  override def $id: String      = id
  override def $key: String     = key
  override def $timestamp: Long = start
}
