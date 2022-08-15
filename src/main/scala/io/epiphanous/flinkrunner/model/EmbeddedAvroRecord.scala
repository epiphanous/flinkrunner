package io.epiphanous.flinkrunner.model

import org.apache.avro.generic.GenericRecord

/** Event types that wrap avro records should implement this trait to
  * support avro serialization with
  * ConfluentAvroRegistryKafkaRecordSerializationSchema. A companion trait,
  * EmbeddedAvroRecordFactory, can be used to support deserializing avro
  * records into flink events.
 *
  * @tparam A
  *   An avro record type
  */
trait EmbeddedAvroRecord[A <: GenericRecord] {

  /** An optional embedded record key - if present, used as the key when
    * stored in kafka. Defaults to None.
    * @return
    *   Option[String]
    */
  def $recordKey: Option[String] = None

  /** The wrapped avro record
    * @return
    *   A
    */
  def $record: A

  /** A map of headers to publish with the avro record in kafka. Defaults
    * to an empty map.
    * @return
    *   Map[String,String]
    */
  def $recordHeaders: Map[String, String] = Map.empty

  /** A helper method for serialization that returns the headers, key, and
    * record.
    * @return
    *   EmbeddedAvroRecordInfo[A]
    */
  def toKV: EmbeddedAvroRecordInfo[A] =
    EmbeddedAvroRecordInfo($record, $recordKey, $recordHeaders)
}
