package io.epiphanous.flinkrunner.model

import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.avro.AvroInputFormat

class EmbeddedAvroInputFormat[
    E <: ADT with EmbeddedAvroRecord[A],
    A <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent](path: String)(implicit
    fromKV: EmbeddedAvroRecordInfo[A] => E)
    extends FileInputFormat[E] {
  val avroInputFormat =
    new AvroInputFormat(
      new Path(path),
      implicitly[TypeInformation[A]].getTypeClass
    )

  override def reachedEnd(): Boolean = avroInputFormat.reachedEnd()

  override def nextRecord(reuse: E): E =
    fromKV(
      EmbeddedAvroRecordInfo(avroInputFormat.nextRecord(reuse.$record))
    )
}
