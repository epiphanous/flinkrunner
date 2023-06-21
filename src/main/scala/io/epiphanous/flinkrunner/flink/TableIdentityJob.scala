package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model.{EmbeddedRowType, FlinkEvent}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream

class TableIdentityJob[
    E <: ADT with EmbeddedRowType: TypeInformation,
    ADT <: FlinkEvent: TypeInformation](runner: FlinkRunner[ADT])
    extends TableStreamJob[E, ADT](runner) {
  override def transform: DataStream[E] = singleSource[E]()
}
