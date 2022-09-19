package io.epiphanous.flinkrunner.model

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase
import org.apache.flink.api.common.serialization.BulkWriter
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.fs.FSDataOutputStream
import org.apache.flink.formats.avro.AvroWriters
import org.apache.flink.formats.parquet.avro.AvroParquetWriters

/** An avro parquet writer factory for types that wrap an avro record.
  *
  * Important: Note that if the embedded avro type is GenericRecord, the
  * caller must provide a schema to the constructor of this factory. This
  * implies that you can only write one type of object into the parquet
  * file factory. This makes sense for a columnar file format.
  *
  * @param optSchema
  *   An optional avro schema describing the type being written to the
  *   parquet file. The caller must provide a schema if A =:=
  *   GenericRecord.
  * @tparam E
  *   A member of an ADT that implements EmbeddedAvroRecord[A]
  * @tparam A
  *   The avro type embedded in E
  * @tparam ADT
  *   The algebraic data type that implements FlinkEvent
  */
class EmbeddedAvroWriterFactory[
    E <: ADT with EmbeddedAvroRecord[A],
    A <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent](
    isParquet: Boolean,
    optSchema: Option[Schema] = None)
    extends BulkWriter.Factory[E] {

  val typeClass: Class[A] = implicitly[TypeInformation[A]].getTypeClass

  require(
    optSchema.nonEmpty || classOf[SpecificRecordBase].isAssignableFrom(
      typeClass
    ),
    s"EmbeddedAvroWriterFactory requires a unique avro schema when the embedded avro type is GenericRecord"
  )

  val schema: Schema = optSchema.getOrElse(
    typeClass
      .getConstructor()
      .newInstance()
      .getSchema
  )

  val avroWriterFactory: BulkWriter.Factory[GenericRecord] =
    if (isParquet)
      AvroParquetWriters.forGenericRecord(schema)
    else AvroWriters.forGenericRecord(schema)

  override def create(out: FSDataOutputStream): BulkWriter[E] = {
    val avroWriter = avroWriterFactory.create(out)
    new BulkWriter[E] {
      override def addElement(element: E): Unit =
        avroWriter.addElement(element.$record)

      override def flush(): Unit = avroWriter.flush()

      override def finish(): Unit = avroWriter.finish()
    }
  }
}
