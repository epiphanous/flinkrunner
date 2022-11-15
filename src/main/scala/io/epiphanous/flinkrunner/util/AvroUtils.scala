package io.epiphanous.flinkrunner.util

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.{
  EmbeddedAvroRecord,
  EmbeddedAvroRecordInfo,
  FlinkConfig,
  FlinkEvent
}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase

import scala.collection.JavaConverters._
import scala.util.Try

object AvroUtils extends LazyLogging {

  def isGeneric[A <: GenericRecord](typeClass: Class[A]): Boolean =
    !isSpecific(typeClass)

  def isSpecific[A <: GenericRecord](typeClass: Class[A]): Boolean =
    classOf[SpecificRecordBase].isAssignableFrom(typeClass)

  def isGenericInstance[A <: GenericRecord](instance: A): Boolean  =
    !isSpecificInstance(instance)
  def isSpecificInstance[A <: GenericRecord](instance: A): Boolean =
    isSpecific(instance.getClass)

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
      config: FlinkConfig,
      keyOpt: Option[String] = None,
      headers: Map[String, String] = Map.empty)(implicit
      fromKV: EmbeddedAvroRecordInfo[A] => E): E =
    if (isGeneric(typeClass) || isSpecificInstance(genericRecord))
      fromKV(
        EmbeddedAvroRecordInfo(
          genericRecord.asInstanceOf[A],
          config,
          keyOpt,
          headers
        )
      )
    else
      fromKV(
        EmbeddedAvroRecordInfo(
          genericRecord.toSpecific(instanceOf(typeClass)),
          config,
          keyOpt,
          headers
        )
      )

  implicit class RichGenericRecord(genericRecord: GenericRecord) {
    def getDataAsSeq[A <: GenericRecord]: Seq[AnyRef] =
      genericRecord.getSchema.getFields.asScala.map(f =>
        genericRecord.get(f.pos())
      )

    def getDataAsMap[A <: GenericRecord]: Map[String, AnyRef] =
      genericRecord.getSchema.getFields.asScala
        .map(f => (f.name(), genericRecord.get(f.name())))
        .toMap

    def toSpecific[A <: GenericRecord](instance: A): A = {
      genericRecord.getSchema.getFields.asScala
        .foldLeft(instance) { (a, field) =>
          val f = field.name()
          genericRecord.get(f) match {
            case rec: GenericRecord =>
              val fieldClassName = rec.getSchema.getFullName
              Try(Class.forName(fieldClassName)).fold(
                error =>
                  logger.error(
                    s"can't convert embedded generic record to a $fieldClassName",
                    error
                  ),
                klass => {
                  if (isGenericInstance(rec)) {
                    val k = klass
                      .getDeclaredConstructor()
                      .newInstance()
                      .asInstanceOf[GenericRecord]
                    a.put(f, rec.toSpecific(k))
                  } else a.put(f, rec)
                }
              )
            case v                  => a.put(f, v)
          }
          a
        }
    }
  }

}
