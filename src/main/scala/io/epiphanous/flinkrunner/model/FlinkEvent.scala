package io.epiphanous.flinkrunner.model

import org.apache.avro.reflect.AvroIgnore

/**
 * A trait defining a flink runner event
 */
trait FlinkEvent extends Product with Serializable {

  /** unique event id */
  @AvroIgnore def $id: String

  /** partition key */
  @AvroIgnore def $key: String

  /** event timestamp */
  @AvroIgnore def $timestamp: Long

  /** true if event is active (used in some flink jobs) */
  @AvroIgnore def $active: Boolean = false

}
