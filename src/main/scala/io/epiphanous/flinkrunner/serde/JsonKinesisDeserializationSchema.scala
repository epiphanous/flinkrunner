package io.epiphanous.flinkrunner.serde

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkEvent
import io.epiphanous.flinkrunner.model.source.KinesisSourceConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema

class JsonKinesisDeserializationSchema[E <: FlinkEvent: TypeInformation](
    kinesisSourceConfig: KinesisSourceConfig)
    extends KinesisDeserializationSchema[E]
    with LazyLogging {

  val deserializationSchema =
    new JsonDeserializationSchema[E](kinesisSourceConfig)

  override def deserialize(
      recordValue: Array[Byte],
      partitionKey: String,
      seqNum: String,
      approxArrivalTimestamp: Long,
      stream: String,
      shardId: String): E =
    deserializationSchema.deserialize(recordValue)

  override def getProducedType: TypeInformation[E] =
    deserializationSchema.getProducedType
}
