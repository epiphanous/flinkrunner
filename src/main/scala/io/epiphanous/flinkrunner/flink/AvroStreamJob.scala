package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model.{EmbeddedAvroRecord, FlinkEvent}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream

/** A [[StreamJob]] with Avro inputs and outputs
  * @param runner
  *   an instance of [[FlinkRunner]]
  * @tparam OUT
  *   the output type, with an embedded avro record of type A
  * @tparam A
  *   the type of avro record that is embedded in our output type. only
  *   this avro part will be written to the sink.
  * @tparam ADT
  *   the algebraic data type of the [[FlinkRunner]] instance
  */
abstract class AvroStreamJob[
    OUT <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
    A <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent: TypeInformation](runner: FlinkRunner[ADT])
    extends StreamJob[OUT, ADT](runner) {

  override def sink(out: DataStream[OUT]): Unit = {
    runner.mainSinkConfigs.foreach(_.addAvroSink[OUT, A](out))
    if (runner.sideSinkConfigs.nonEmpty) sinkSideOutputs(out)
  }
}
