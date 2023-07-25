package io.epiphanous.flinkrunner.model

import enumeratum.EnumEntry.{Lowercase, Uppercase}
import enumeratum.{Enum, EnumEntry}

import scala.collection.immutable

sealed trait SchemaRegistryType
    extends EnumEntry
    with Lowercase
    with Uppercase

object SchemaRegistryType extends Enum[SchemaRegistryType] {

  case object Confluent extends SchemaRegistryType
  case object AwsGlue   extends SchemaRegistryType

  override def values: immutable.IndexedSeq[SchemaRegistryType] =
    findValues
}
