package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.util.AvroUtils
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.file.src.reader.StreamFormat
import org.apache.flink.core.fs.FSDataInputStream
import org.apache.flink.formats.parquet.avro.AvroParquetReaders

/** A StreamFormat to read avro parquet files containing a type that embeds
  * an avro record.
  * @param fromKV
  *   An implicitly provided method for creating an instance of type E from
  *   an avro record of type A. This is provided by having your E type's
  *   companion object extend the EmbeddedAvroRecordFactory trait.
  * @tparam E
  *   a type in ADT that embeds an avro record of type A
  * @tparam A
  *   an avro record type
  * @tparam ADT
  *   a flink event algebraic data type
  */
class EmbeddedAvroParquetInputFormat[
    E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
    A <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent](optSchema: Option[Schema] = None)(implicit
    fromKV: EmbeddedAvroRecordInfo[A] => E)
    extends StreamFormat[E] {

  val typeClass: Class[A] = implicitly[TypeInformation[A]].getTypeClass

  require(
    optSchema.nonEmpty || AvroUtils.isSpecific(typeClass),
    s"EmbeddedAvroParquetInputFormat requires a unique avro schema when the embedded avro type is GenericRecord"
  )

  /** An avro parquet stream format to read parquet events into avro
    * generic records.
    *
    * NOTE: Flink requires a schema before any files are read to wire up
    * the job. If the embedded avro type is a specific record, we will use
    * the schema defined in that class definition. If the embedded record,
    * however is generic, callers must provide a schema.
    */
  val avroParquetFormat: StreamFormat[GenericRecord] = AvroParquetReaders
    .forGenericRecord(
      optSchema.getOrElse(AvroUtils.instanceOf(typeClass).getSchema)
    )

  override def createReader(
      config: Configuration,
      stream: FSDataInputStream,
      fileLen: Long,
      splitEnd: Long): StreamFormat.Reader[E] = getReader(
    avroParquetFormat.createReader(config, stream, fileLen, splitEnd)
  )

  override def restoreReader(
      config: Configuration,
      stream: FSDataInputStream,
      restoredOffset: Long,
      fileLen: Long,
      splitEnd: Long): StreamFormat.Reader[E] =
    getReader(
      avroParquetFormat.restoreReader(
        config,
        stream,
        restoredOffset,
        fileLen,
        splitEnd
      )
    )

  override def isSplittable: Boolean = avroParquetFormat.isSplittable

  override def getProducedType: TypeInformation[E] =
    implicitly[TypeInformation[E]]

  /** Return a StreamFormat.Reader for a type that wraps an avro record,
    * using a fromKV method that must be available in implicit scope.
    * @param avroReader
    *   a StreamFormat.Reader for the avro record type A
    * @return
    *   StreamFormat.Reader[E]
    */
  private def getReader(avroReader: StreamFormat.Reader[GenericRecord])
      : StreamFormat.Reader[E] =
    new StreamFormat.Reader[E] {
      override def read(): E =
        AvroUtils.toEmbeddedAvroInstance[E, A, ADT](
          avroReader.read(),
          typeClass
        )

      override def close(): Unit = avroReader.close()
    }
}
