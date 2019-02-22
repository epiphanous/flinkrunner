package io.epiphanous.flinkrunner
import io.epiphanous.flinkrunner.flink.FlinkJob
import io.epiphanous.flinkrunner.model.FlinkEvent
import io.epiphanous.flinkrunner.operator.AddToJdbcBatchFunction
import org.apache.flink.api.common.serialization.{DeserializationSchema, Encoder, SerializationSchema}
import org.apache.flink.streaming.util.serialization.{KeyedDeserializationSchema, KeyedSerializationSchema}

trait FlinkRunnerFactory[ADT <: FlinkEvent] {

  def getJobInstance(name: String): FlinkJob[_ <: ADT, _ <: ADT]

  def getDeserializationSchema: DeserializationSchema[ADT] = ???

  def getKeyedDeserializationSchema: KeyedDeserializationSchema[ADT] = ???

  def getSerializationSchema: SerializationSchema[ADT] = ???

  def getKeyedSerializationSchema: KeyedSerializationSchema[ADT] = ???

  def getEncoder: Encoder[ADT] = ???

  def getAddToJdbcBatchFunction: AddToJdbcBatchFunction[ADT] = ???
}
