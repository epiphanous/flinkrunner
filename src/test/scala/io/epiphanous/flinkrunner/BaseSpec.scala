package io.epiphanous.flinkrunner

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkEvent,
  MySimpleADT,
  NothingADT
}
import org.apache.flink.api.common.JobExecutionResult
import org.scalatest.matchers.should.Matchers
import org.scalatest.{EitherValues, Inside, OptionValues, TryValues}

trait BaseSpec
    extends Matchers
    with OptionValues
    with EitherValues
    with TryValues
    with Inside
    with LazyLogging {

  def getRunner[ADT <: FlinkEvent](
      args: Array[String] = Array.empty,
      optConfig: Option[String] = None): FlinkRunner[ADT] = {
    val config = new FlinkConfig(args, optConfig)
    new FlinkRunner[ADT](config) {
      override def invoke(
          jobName: String): Either[List[_], JobExecutionResult] = ???
    }
  }

  lazy val nothingFlinkRunner: FlinkRunner[NothingADT] =
    getRunner[NothingADT]()

  lazy val mySimpleFlinkRunner: FlinkRunner[MySimpleADT] =
    getRunner[MySimpleADT]()
}
