package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.flink.StreamJob
import io.epiphanous.flinkrunner.{FlinkRunner, FlinkRunnerFactory}

class NoJobFactory[ADT <: FlinkEvent] extends FlinkRunnerFactory[ADT] {
  override def getJobInstance[OUT <: ADT](
      name: String,
      runner: FlinkRunner[ADT]): StreamJob[OUT, ADT] = ???
}
