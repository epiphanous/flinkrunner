package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model.{EmbeddedAvroRecord, FlinkEvent}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream

/** A job class to generate streaming output tables from an event that
  * contains an embedded avro type. The advantage of this class, versus
  * [[TableStreamJob]] is the row type of the output is inferred from the
  * avro type and doesn't require you to specify it in the sink
  * configuration.
  *
  * @param runner
  *   an instance of [[FlinkRunner]]
  * @tparam OUT
  *   the output type implementing embedded avro record
  * @tparam OUTA
  *   the embedded avro type
  * @tparam ADT
  *   the algebraic data type of the [[FlinkRunner]] instance
  */
abstract class AvroTableStreamJob[
    OUT <: ADT with EmbeddedAvroRecord[OUTA]: TypeInformation,
    OUTA <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent: TypeInformation](runner: FlinkRunner[ADT])
    extends AvroStreamJob[OUT, OUTA, ADT](runner) {

  override def sink(out: DataStream[OUT]): Unit =
    runner.getSinkNames.foreach(name =>
      runner.addAvroRowSink[OUT, OUTA](out, name)
    )
}
