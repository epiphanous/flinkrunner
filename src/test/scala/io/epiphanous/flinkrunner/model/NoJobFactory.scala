package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.flink.BaseFlinkJob
import io.epiphanous.flinkrunner.{FlinkRunner, FlinkRunnerFactory}

class NoJobFactory[ADT <: FlinkEvent] extends FlinkRunnerFactory[ADT] {
  override def getJobInstance[DS, OUT <: ADT](
      name: String,
      runner: FlinkRunner[ADT]): BaseFlinkJob[DS, OUT, ADT] = ???
}
