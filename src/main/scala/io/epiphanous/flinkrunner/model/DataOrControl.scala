package io.epiphanous.flinkrunner.model

import org.apache.flink.api.common.typeinfo.TypeInformation

case class DataOrControl[D <: ADT, C <: ADT, ADT <: FlinkEvent](
    event: Either[D, C])
    extends FlinkEvent {
  def $id: String = event.fold(_.$id, _.$id)

  def $key: String = event.fold(_.$key, _.$key)

  def $timestamp: Long = event.fold(_.$timestamp, _.$timestamp)

  override def $active: Boolean = event.fold(_.$active, _.$active)

  def isControl: Boolean = event.isRight

  def isData: Boolean = event.isLeft

  def data: Either.LeftProjection[D, C] = event.left

  def control: Either.RightProjection[D, C] = event.right

  def info: String = event.fold(_.toString, _.toString)
}

object DataOrControl {
  def data[D <: ADT, C <: ADT, ADT <: FlinkEvent: TypeInformation](
      data: D): DataOrControl[D, C, ADT] =
    DataOrControl[D, C, ADT](Left(data))

  def control[D <: ADT, C <: ADT, ADT <: FlinkEvent: TypeInformation](
      control: C) =
    DataOrControl[D, C, ADT](Right(control))
}
