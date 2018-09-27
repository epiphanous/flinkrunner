package io.epiphanous.flinkrunner.model

case class DataOrControl[D <: FlinkEvent, C <: FlinkEvent](event: Either[D, C]) extends FlinkEvent {
  def $key: String                          = event.fold(_.$key, _.$key)
  def $timestamp: Long                      = event.fold(_.$timestamp, _.$timestamp)
  override def $active: Boolean             = event.fold(_.$active, _.$active)
  def isControl: Boolean                    = event.isRight
  def isData: Boolean                       = event.isLeft
  def data: Either.LeftProjection[D, C]     = event.left
  def control: Either.RightProjection[D, C] = event.right
  def info: String                          = event.fold(_.toString, _.toString)
}

object DataOrControl {
  def data[D <: FlinkEvent, C <: FlinkEvent](event: D): DataOrControl[D, C]    = DataOrControl[D, C](Left(event))
  def control[D <: FlinkEvent, C <: FlinkEvent](event: C): DataOrControl[D, C] = DataOrControl[D, C](Right(event))
}
