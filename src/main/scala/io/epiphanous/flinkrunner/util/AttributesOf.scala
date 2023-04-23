package io.epiphanous.flinkrunner.util

import shapeless._
import shapeless.ops.hlist
import shapeless.ops.record._

trait AttributesOf[T] {
  def names: List[String]
}

object AttributesOf {
  implicit def toAttributes[T, Repr <: HList, KeysRepr <: HList](implicit
      gen: LabelledGeneric.Aux[T, Repr],
      keys: Keys.Aux[Repr, KeysRepr],
      traversable: hlist.ToTraversable.Aux[KeysRepr, List, Symbol])
      : AttributesOf[T] =
    new AttributesOf[T] {
      override def names: List[String] = keys().toList.map(_.name)
    }

  def apply[T](implicit attributesOf: AttributesOf[T]): AttributesOf[T] =
    attributesOf

}

object Fields {
  def namesOf[A <: Product](a: A)(implicit
      attributesOf: AttributesOf[A]): List[String] = attributesOf.names

  def of[A <: Product](a: A)(implicit
      attributesOf: AttributesOf[A]): List[(String, Any)] =
    namesOf(a) zip a.productIterator.toList
}
