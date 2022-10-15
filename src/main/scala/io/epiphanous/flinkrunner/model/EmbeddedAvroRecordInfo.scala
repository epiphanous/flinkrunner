package io.epiphanous.flinkrunner.model

import org.apache.avro.generic.GenericRecord

/** Information to support serialization/deserialization of event types
  * that wrap avro records
  *
  * @param record
  *   the avro record
  * @param keyOpt
  *   optional string key
  * @param headers
  *   kafka headers as Map[String,String]
  * @tparam A
  *   the avro record type
  */
case class EmbeddedAvroRecordInfo[A <: GenericRecord](
    record: A,
    config: FlinkConfig,
    keyOpt: Option[String] = None,
    headers: Map[String, String] = Map.empty
)
