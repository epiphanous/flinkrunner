package io.epiphanous.flinkrunner

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.flink.{BaseFlinkJob, FlinkJob}
import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkEvent}
import org.apache.flink.streaming.api.scala.DataStream
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
      override def getJobInstance[DS, OUT <: NothingADT](
          name: String,
          runner: FlinkRunner[NothingADT])
          : BaseFlinkJob[DS, OUT, NothingADT] = ???
    }

  implicit val nothingConfig: FlinkConfig =
    new FlinkConfig(Array.empty[String])

  val nothingFlinkRunner =
    new FlinkRunner[NothingADT](Array.empty[String], nothingFactory)

}
