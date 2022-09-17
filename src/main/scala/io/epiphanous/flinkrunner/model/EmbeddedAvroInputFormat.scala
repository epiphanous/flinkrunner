package io.epiphanous.flinkrunner.model

import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase
import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.fs.{FileInputSplit, Path}
import org.apache.flink.formats.avro.AvroInputFormat

import collection.JavaConverters._

/** An input format to read avro files into a flink event that embeds an
  * avro record.
  * @param path
  *   a flink path to the file or directory containing the files
  * @param fromKV
  *   a method to create an instance of the flink event from an avro record
  * @tparam E
  *   the flink event that extends EmbeddedAvroRecord[A]
  * @tparam A
  *   the embedded avro type
  * @tparam ADT
  *   the flink event ADT
  */
class EmbeddedAvroInputFormat[
    E <: ADT with EmbeddedAvroRecord[A],
    A <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent](path: Path)(implicit
    fromKV: EmbeddedAvroRecordInfo[A] => E)
    extends FileInputFormat[E] {

  setFilePath(path)

  @transient
  lazy val typeClass: Class[A] =
    implicitly[TypeInformation[A]].getTypeClass

  def recordInstance: A = typeClass.getConstructor().newInstance()

  @transient
  lazy val fieldNames: Seq[String] =
    if (classOf[SpecificRecordBase].isAssignableFrom(typeClass))
      recordInstance.getSchema.getFields.asScala
        .map(_.name())
    else Seq.empty

  @transient
  lazy val isGeneric: Boolean = fieldNames.isEmpty

  val avroInputFormat: AvroInputFormat[GenericRecord] = {
    val inputFormat = new AvroInputFormat(
      path,
      classOf[GenericRecord]
    )
    inputFormat.setNestedFileEnumeration(true)
    inputFormat
  }

  override def open(fileSplit: FileInputSplit): Unit = {
    super.open(fileSplit)
    avroInputFormat.open(fileSplit)
  }

  override def reachedEnd(): Boolean = avroInputFormat.reachedEnd()

  override def nextRecord(reuse: E): E = {
    // this painful dance is because avro can't reflect on a scala case class and
    // find the SCHEMA$ class, so throws an AvroRuntimeException: Not a Specific Class
    val genericRecord = avroInputFormat.nextRecord(reuse.$record)
    if (isGeneric)
      fromKV(EmbeddedAvroRecordInfo(genericRecord.asInstanceOf[A]))
    else {
      val specificRecord: A =
        fieldNames
          .foldLeft(recordInstance) { case (a, f) =>
            a.put(f, genericRecord.get(f))
            a
          }
      fromKV(
        EmbeddedAvroRecordInfo(specificRecord)
      )
    }
  }
}
