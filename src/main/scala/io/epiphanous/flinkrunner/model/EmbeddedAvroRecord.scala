package io.epiphanous.flinkrunner.model

import org.apache.avro.generic.GenericRecord
import org.apache.flink.types.Row

import scala.collection.JavaConverters._

/** Event types that wrap avro records should implement this trait. This
  * trait works with other avro related features in Flinkrunner, such as
  * avro serdes for confluent schema registry and reading and writing avro
  * and parquet files. A companion trait, EmbeddedAvroRecordFactory, can be
  * used to support deserializing avro records into flink events.
  *
  * The embedded type can be a specific record or a generic record. Because
  * generated specific record types are already associated with a schema,
  * while GenericRecord is not, it is usually preferable to use specific
  * types, via code generation. If you need to embed a generic record, you
  * will have to provide a schema at runtime, which is sometimes less
  * convenient.
  *
  * Note: EmbeddedAvroRecords automatically inherit from EmbeddedRowType,
  * which means they have a `toRow` method and can be used with
  * AvroTableStreamJobs.
  *
  * @tparam A
  *   An avro record type
  */
trait EmbeddedAvroRecord[A <: GenericRecord] extends EmbeddedRowType {

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
  def toKV(config: FlinkConfig): EmbeddedAvroRecordInfo[A] =
    EmbeddedAvroRecordInfo($record, config, $recordKey, $recordHeaders)

  def toRow: Row =
    $record.getSchema.getFields.asScala
      .map(_.name())
      .foldLeft(Row.withNames(rowKind)) { case (row, field) =>
        row.setField(field, $record.get(field))
        row
      }
}
