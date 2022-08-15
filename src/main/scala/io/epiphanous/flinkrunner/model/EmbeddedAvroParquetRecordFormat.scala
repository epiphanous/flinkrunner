package io.epiphanous.flinkrunner.model

import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.file.src.reader.StreamFormat
import org.apache.flink.core.fs.FSDataInputStream

/** A StreamFormat to read parquet avro files containing a type that embeds
  * an avro record.
  * @param fromKV
  *   An implicitly provided method for creating an instance of type E from
  *   an avro record of type A. This is provided by having your E type's
  *   companion object extend the EmbeddedAvroRecordFactory trait.
  * @param avroParquetRecordFormat
  *   An implicitly provided avro parquet record format to read avro
  *   records from a parquet file. This can be provided by having your E
  *   type's companion object extend the EmbeddedAvroRecordFactory trait
  *   and implement it's avroParquetRecordFormat member.
  * @tparam E
  *   a type that is an ADT and embeds an avro record of type A
  * @tparam A
  *   an avro record type (specific type)
  * @tparam ADT
  *   a flink event algebraic data type
  */
class EmbeddedAvroParquetRecordFormat[
    E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
    A <: GenericRecord,
    ADT <: FlinkEvent](implicit
    fromKV: EmbeddedAvroRecordInfo[A] => E,
    val avroParquetRecordFormat: StreamFormat[A])
    extends StreamFormat[E] {

  override def createReader(
      config: Configuration,
      stream: FSDataInputStream,
      fileLen: Long,
      splitEnd: Long): StreamFormat.Reader[E] = getReader(
    avroParquetRecordFormat.createReader(config, stream, fileLen, splitEnd)
  )

  override def restoreReader(
      config: Configuration,
      stream: FSDataInputStream,
      restoredOffset: Long,
      fileLen: Long,
      splitEnd: Long): StreamFormat.Reader[E] =
    getReader(
      avroParquetRecordFormat.restoreReader(
        config,
        stream,
        restoredOffset,
        fileLen,
        splitEnd
      )
    )

  override def isSplittable: Boolean = avroParquetRecordFormat.isSplittable

  override def getProducedType: TypeInformation[E] =
    implicitly[TypeInformation[E]]

  /** Return a StreamFormat.Reader for a type that wraps an avro record,
    * using a fromKV method that must be available in implicit scope.
    * @param avroReader
    *   a StreamFormat.Reader for the avro specific record type A
    * @return
    *   StreamFormat.Reader[E]
    */
  private def getReader(
      avroReader: StreamFormat.Reader[A]): StreamFormat.Reader[E] =
    new StreamFormat.Reader[E] {
      override def read(): E =
        fromKV(EmbeddedAvroRecordInfo(avroReader.read()))

      override def close(): Unit = avroReader.close()
    }
}
