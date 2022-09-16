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

  /** A stream format for reading avro records from parquet files.
    */
  implicit val avroParquetRecordFormatOpt: Option[StreamFormat[A]] = None
}
