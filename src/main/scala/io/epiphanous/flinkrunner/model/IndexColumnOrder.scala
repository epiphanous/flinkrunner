package io.epiphanous.flinkrunner.model

import scala.language.implicitConversions

sealed trait IndexColumnOrder
case object ASC  extends IndexColumnOrder
case object DESC extends IndexColumnOrder

object IndexColumnOrder {
  implicit def stringToOrder(s: String): IndexColumnOrder =
    s.toUpperCase match {
      case "ASC" | "A"  => ASC
      case "DESC" | "D" => DESC
    }
}
