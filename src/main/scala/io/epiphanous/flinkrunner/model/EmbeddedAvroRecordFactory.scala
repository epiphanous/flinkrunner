package io.epiphanous.flinkrunner.model

import org.apache.avro.generic.GenericRecord
import org.apache.flink.connector.file.src.reader.StreamFormat

/** Companion objects of event types that wrap avro records should
  * implement this trait to support avro deserialization with
  * ConfluentAvroRegistryKafkaDeserializationSchema. A companion trait,
  * EmbeddedAvroRecord, can be used to support serializing avro records
  * from the flink events that implement it.
  * @tparam E
  *   a flink event that implements EmbeddedAvroRecord[A]
  * @tparam A
  *   An avro record type
  */
trait EmbeddedAvroRecordFactory[
    E <: FlinkEvent with EmbeddedAvroRecord[A],
    A <: GenericRecord] {

  /** Construct an event of type E from kafka headers, an optional key, and
    * an avro record.
    * @param recordInfo
    *   embedded avro record info needed to construct an event of type E
    * @return
    *   New event of type E
    */
  implicit def fromKV(recordInfo: EmbeddedAvroRecordInfo[A]): E

  /** A stream format for reading avro records from parquet files. This is
    * only needed in cases where you have an avro parquet source. You can
    * provide this simply by using
    *
    * {{{AvroParquetReaders.forSpecificRecord(YOUR-AVRO-CLASS)}}}
    *
    * or {{{AvroParquetReaders.forGenericRecord(YOUR-SCHEMA)}}}
    *
    * depending on whether your embedded avro type is a specific or generic
    * record. It usually will be specific.
    *
    * This is another leak in this leaky abstraction of embedded avro
    * records, another annoying bit of boilerplate. But it's required
    * because of Java's type erasure on the generic embedded avro type A.
    *
    * Important: This is intentionally unimplemented so we don't force
    * implementers of this trait to provide one, but throw if it's needed,
    * but not provided by the implementer
    */
  implicit def avroParquetRecordFormat: StreamFormat[A] = ???
}
