package io.epiphanous.flinkrunner

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.flink.BaseFlinkJob
import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkEvent}
import org.scalatest._
import org.scalatest.matchers.should.Matchers

trait BaseSpec
    extends Matchers
    with OptionValues
    with EitherValues
    with TryValues
    with Inside
    with LazyLogging {

  sealed trait NothingADT extends FlinkEvent

  val nothingFactory: FlinkRunnerFactory[NothingADT] =
    new FlinkRunnerFactory[NothingADT] {
      override def getJobInstance(
          name: String,
          config: FlinkConfig): BaseFlinkJob[_, _ <: NothingADT] = ???
    }

  implicit val nothingConfig: FlinkConfig =
    new FlinkConfig(Array.empty[String], nothingFactory)

  val nothingFlinkRunner =
    new FlinkRunner[NothingADT](Array.empty[String], nothingFactory)

}
