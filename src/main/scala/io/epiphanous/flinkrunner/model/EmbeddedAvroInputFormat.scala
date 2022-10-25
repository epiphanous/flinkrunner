package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.model.source.FileSourceConfig
import io.epiphanous.flinkrunner.util.AvroUtils
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.fs.{FileInputSplit, Path}
import org.apache.flink.formats.avro.AvroInputFormat

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
    ADT <: FlinkEvent](sourceConfig: FileSourceConfig[ADT])(implicit
    fromKV: EmbeddedAvroRecordInfo[A] => E)
    extends FileInputFormat[E] {

  setFilePath(sourceConfig.origin)
  setNestedFileEnumeration(true)
  if (sourceConfig.wantsFiltering) setFilesFilter(sourceConfig.fileFilter)

  @transient
  lazy val typeClass: Class[A] =
    implicitly[TypeInformation[A]].getTypeClass

  val avroInputFormat: AvroInputFormat[GenericRecord] = {
    val inputFormat = new AvroInputFormat(
      sourceConfig.origin,
      classOf[GenericRecord]
    )
//    inputFormat.setNestedFileEnumeration(true)
    inputFormat
  }

  override def open(fileSplit: FileInputSplit): Unit =
//    super.open(fileSplit)
    avroInputFormat.open(fileSplit)

  override def close(): Unit =
//    super.close()
    avroInputFormat.close()

  override def reachedEnd(): Boolean = avroInputFormat.reachedEnd()

  override def nextRecord(reuse: E): E = {
    Option(avroInputFormat.nextRecord(reuse.$record))
      .map(record =>
        AvroUtils.toEmbeddedAvroInstance[E, A, ADT](
          record,
          typeClass,
          sourceConfig.config
        )
      )
      .getOrElse(null.asInstanceOf[E]) // fugly to compile
  }

}
