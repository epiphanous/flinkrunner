package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.util.InstantUtils.RichInstant
import org.apache.avro.reflect.AvroIgnore

import java.time.Instant

/** A trait defining a flink runner event
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

  /** a path for bucketing streams of this event */
  @AvroIgnore def $bucketId: String =
    Instant.ofEpochMilli($timestamp).prefixedTimePath(s"${$key}/")

  /** an id to use to deduplicate a stream of events */
  @AvroIgnore def $dedupeId: String = $id

}
