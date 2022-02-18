package io.epiphanous.flinkrunner

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.flink.StreamJob
import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkEvent,
  MySimpleADT,
  NothingADT
}
import org.scalatest.matchers.should.Matchers
import org.scalatest.{EitherValues, Inside, OptionValues, TryValues}

trait BaseSpec
    extends Matchers
    with OptionValues
    with EitherValues
    with TryValues
    with Inside
    with LazyLogging {

  val nothingFactory: FlinkRunnerFactory[NothingADT] =
    new FlinkRunnerFactory[NothingADT] {
      override def getJobInstance[OUT <: NothingADT](
          name: String,
          runner: FlinkRunner[NothingADT]): StreamJob[OUT, NothingADT] =
        ???
    }

  implicit val nothingConfig: FlinkConfig =
    new FlinkConfig(Array.empty[String])

  val nothingFlinkRunner =
    new FlinkRunner[NothingADT](Array.empty[String], nothingFactory)

  val mySimpleFactory: FlinkRunnerFactory[MySimpleADT] =
    new FlinkRunnerFactory[MySimpleADT] {
      override def getJobInstance[OUT <: MySimpleADT](
          name: String,
          runner: FlinkRunner[MySimpleADT]): StreamJob[OUT, MySimpleADT] =
        ???
    }
  val mySimpleConfig: FlinkConfig                      = new FlinkConfig(Array.empty[String])
  val mySimpleFlinkRunner                              =
    new FlinkRunner[MySimpleADT](Array.empty, mySimpleFactory)
}
