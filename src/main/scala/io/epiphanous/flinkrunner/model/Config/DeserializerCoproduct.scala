package io.epiphanous.flinkrunner.model.Config
import io.epiphanous.flinkrunner.model.FlinkEvent
import io.epiphanous.flinkrunner.operator.AddToJdbcBatchFunction
import org.apache.flink.api.common.serialization.{DeserializationSchema, Encoder}
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema

case class DeserializerCoproduct[E <: FlinkEvent](
    deserializationSchema: Option[DeserializationSchema[E]] = None,
    keyedDeserializationSchema: Option[KeyedDeserializationSchema[E]] = None,
    addToJdbcBatchFunction: Option[AddToJdbcBatchFunction[E]] = None,
    fileEncoder: Option[Encoder[E]] = None)
