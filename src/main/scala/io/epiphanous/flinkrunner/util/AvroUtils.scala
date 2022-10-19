package io.epiphanous.flinkrunner.util

import io.epiphanous.flinkrunner.model.{
  EmbeddedAvroRecord,
  EmbeddedAvroRecordInfo,
  FlinkEvent
}
import org.apache.avro.Schema
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

  def schemaOf[A <: GenericRecord](
      typeClass: Class[A],
      schemaStringOpt: Option[String]): Schema =
    if (isSpecific(typeClass)) instanceOf(typeClass).getSchema
    else
      schemaStringOpt
        .map(parseSchemaString)
        .getOrElse(throw new RuntimeException("missing schema"))

  def parseSchemaString(schemaStr: String): Schema =
    new Schema.Parser().parse(schemaStr)

  def subjectName[A <: GenericRecord](
      typeClass: Class[A],
      schemaOpt: Option[Either[String, Schema]] = None,
      isKey: Boolean = false): Option[String] = {
    val suffix = if (isKey) "-key" else "-value"
    val name   =
      if (isSpecific(typeClass)) Some(typeClass.getCanonicalName)
      else
        schemaOpt
          .map(strOrSchema =>
            strOrSchema
              .fold(str => parseSchemaString(str), schema => schema)
              .getFullName
          )
    name.map(_ + suffix)
  }

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
      typeClass: Class[A],
      keyOpt: Option[String] = None,
      headers: Map[String, String] = Map.empty)(implicit
      fromKV: EmbeddedAvroRecordInfo[A] => E): E =
    if (isGeneric(typeClass))
      fromKV(
        EmbeddedAvroRecordInfo(
          genericRecord.asInstanceOf[A],
          keyOpt,
          headers
        )
      )
    else
      fromKV(
        EmbeddedAvroRecordInfo(
          genericRecord.toSpecific(instanceOf(typeClass)),
          keyOpt,
          headers
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
