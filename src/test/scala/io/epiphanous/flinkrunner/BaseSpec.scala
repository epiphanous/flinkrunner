package io.epiphanous.flinkrunner

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
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

  implicit val nothingConfig: FlinkConfig =
    new FlinkConfig(Array.empty[String])

  val nothingFlinkRunner: FlinkRunner[NothingADT] =
    new FlinkRunner[NothingADT](nothingConfig) {
      override def invoke(
          jobName: String): Either[List[_], JobExecutionResult] = ???
    }

  val mySimpleConfig: FlinkConfig                   =
    new FlinkConfig(Array.empty[String])
  val mySimpleFlinkRunner: FlinkRunner[MySimpleADT] =
    new FlinkRunner[MySimpleADT](mySimpleConfig) {
      override def invoke(
          jobName: String): Either[List[_], JobExecutionResult] = ???
    }

}
