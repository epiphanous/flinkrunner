package io.epiphanous.flinkrunner.model

case class DataControlPeriod[D <: FlinkEvent](key: String,
                                              start: Long = 0L,
                                              end: Long = 0L,
                                              elements: List[D] = List.empty)
    extends FlinkEvent {
  def duration: Long = end - start

  override def $key = key

  override def $timestamp = start
}
