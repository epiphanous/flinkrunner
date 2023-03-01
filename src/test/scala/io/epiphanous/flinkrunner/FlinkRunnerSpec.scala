package io.epiphanous.flinkrunner

import io.epiphanous.flinkrunner.flink.{AvroStreamJob, StreamJob}
import io.epiphanous.flinkrunner.model.{CheckResults, EmbeddedAvroRecord, FlinkConfig, FlinkEvent}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation

class FlinkRunnerSpec extends PropSpec {

  def getStreamJobRunner[
      OUT <: ADT: TypeInformation,
      ADT <: FlinkEvent: TypeInformation](
      configStr: String,
      jobFactory: FlinkRunner[ADT] => StreamJob[OUT, ADT],
      checkResultsOpt: Option[CheckResults[ADT]] = None)
      : FlinkRunner[ADT] = {
    val config = new FlinkConfig(Array("testJob"), Some(configStr))
    new FlinkRunner[ADT](config, checkResultsOpt) {
      override def invoke(jobName: String): Unit = jobName match {
        case "testJob" =>
          logger.debug("invoking job")
          jobFactory(this).run()
        case _         => throw new RuntimeException(s"unknown job $jobName")
      }
    }
  }

  def testStreamJob[
      OUT <: ADT: TypeInformation,
      ADT <: FlinkEvent: TypeInformation](
      configStr: String,
      jobFactory: FlinkRunner[ADT] => StreamJob[OUT, ADT],
      checkResultsOpt: Option[CheckResults[ADT]] = None): Unit =
    getStreamJobRunner(configStr, jobFactory, checkResultsOpt).process()

  def getAvroStreamJobRunner[
      OUT <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation,
      ADT <: FlinkEvent: TypeInformation](
      configStr: String,
      jobFactory: FlinkRunner[ADT] => AvroStreamJob[OUT, A, ADT])
      : FlinkRunner[ADT] = {
    val config = new FlinkConfig(Array("testJob"), Some(configStr))
    new FlinkRunner[ADT](config, None) {
      override def invoke(jobName: String): Unit = jobName match {
        case "testJob" => jobFactory(this).run()
        case _         => throw new RuntimeException(s"unknown job $jobName")
      }
    }
  }

  def testAvroStreamJob[
      OUT <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation,
      ADT <: FlinkEvent: TypeInformation](
      configStr: String,
      jobFactory: FlinkRunner[ADT] => AvroStreamJob[OUT, A, ADT]): Unit =
    getAvroStreamJobRunner(configStr, jobFactory).process()

}
