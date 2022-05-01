package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model.FlinkEvent
import org.apache.avro.specific.SpecificRecordBase
import org.apache.flink.api.common.typeinfo.TypeInformation

/**
 * A [[StreamJob]] whose output type is a subclass of an Avro
 * [[SpecificRecordBase]] class.
 * @param runner
 *   an instance of [[FlinkRunner]]
 * @tparam OUT
 *   the output type, a subclass of SpecificRecordBase
 * @tparam ADT
 *   the algebraic data type of the [[FlinkRunner]] instance
 */
abstract class AvroStreamJob[
    OUT <: SpecificRecordBase: TypeInformation,
    ADT <: FlinkEvent: TypeInformation](runner: FlinkRunner[ADT])
    extends StreamJob[OUT, ADT](runner) {}
