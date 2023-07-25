package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model.{EmbeddedRowType, FlinkEvent}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream

import scala.reflect.runtime.{universe => ru}

class TableIdentityJob[
    E <: ADT with EmbeddedRowType: TypeInformation: ru.TypeTag,
    ADT <: FlinkEvent: TypeInformation](runner: FlinkRunner[ADT])
    extends TableStreamJob[E, ADT](runner) {
  override def transform: DataStream[E] = singleSource[E]()
}
