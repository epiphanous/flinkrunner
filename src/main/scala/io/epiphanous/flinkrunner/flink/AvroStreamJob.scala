package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model.FlinkEvent
import org.apache.avro.specific.SpecificRecordBase
import org.apache.flink.api.common.typeinfo.TypeInformation

abstract class AvroStreamJob[
    OUT <: SpecificRecordBase: TypeInformation,
    ADT <: FlinkEvent: TypeInformation](runner: FlinkRunner[ADT])
    extends StreamJob[OUT, ADT](runner) {}