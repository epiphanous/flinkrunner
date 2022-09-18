package io.epiphanous.flinkrunner.util

import io.epiphanous.flinkrunner.model.{
  EmbeddedAvroRecord,
  EmbeddedAvroRecordInfo,
  FlinkEvent
}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase

import scala.collection.JavaConverters._

object AvroUtils {

  def isGeneric[A <: GenericRecord](typeClass: Class[A]): Boolean =
    !isSpecific(typeClass)

  def isSpecific[A <: GenericRecord](typeClass: Class[A]): Boolean =
    classOf[SpecificRecordBase].isAssignableFrom(typeClass)

  def instanceOf[A <: GenericRecord](typeClass: Class[A]): A =
    typeClass.getConstructor().newInstance()

  /** Converts a generic record into a flink event with an embedded avro
    * record of type A
    * @param genericRecord
    *   a generic avro record
    * @param typeClass
    *   Class[A]
    * @param fromKV
    *   implicitly provided function to create type E from an avro record
    * @tparam E
    *   the event type to create
    * @tparam A
    *   the avro record type
    * @tparam ADT
    *   the flink event ADT
    * @return
    *   E
    */
  def toEmbeddedAvroInstance[
      E <: ADT with EmbeddedAvroRecord[A],
      A <: GenericRecord,
      ADT <: FlinkEvent](
      genericRecord: GenericRecord,
      typeClass: Class[A])(implicit
      fromKV: EmbeddedAvroRecordInfo[A] => E): E =
    if (isGeneric(typeClass))
      fromKV(EmbeddedAvroRecordInfo(genericRecord.asInstanceOf[A]))
    else
      fromKV(
        EmbeddedAvroRecordInfo(
          genericRecord.toSpecific(instanceOf(typeClass))
        )
      )

  implicit class GenericToSpecific(genericRecord: GenericRecord) {
    def toSpecific[A <: GenericRecord](instance: A): A = {
      genericRecord.getSchema.getFields.asScala
        .map(_.name())
        .foldLeft(instance) { (a, f) =>
          a.put(f, genericRecord.get(f))
          a
        }
    }
  }

}
