package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model.FlinkEvent
import org.apache.flink.api.common.typeinfo.TypeInformation

/**
 * A [[StreamJob]] whose output is a subclass of the FlinkRunner algebraic
 * data type.
 * @param runner
 *   an instance of FlinkRunner
 * @tparam OUT
 *   the output type of the job, a subclass of ADT
 * @tparam ADT
 *   the algebraic data type
 */
abstract class ADTStreamJob[
    OUT <: ADT: TypeInformation,
    ADT <: FlinkEvent: TypeInformation](runner: FlinkRunner[ADT])
    extends StreamJob[OUT, ADT](runner) {}
