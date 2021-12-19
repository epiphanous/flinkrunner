package io.epiphanous.flinkrunner.flink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkEvent}
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{
  DataStream,
  StreamExecutionEnvironment
}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import scala.util.Try

abstract class StreamJob[OUT <: ADT: TypeInformation, ADT <: FlinkEvent](
    runner: FlinkRunner[ADT])
    extends LazyLogging {

  val config: FlinkConfig              = runner.config
  val env: StreamExecutionEnvironment  = runner.env
  val tableEnv: StreamTableEnvironment = runner.tableEnv

  def flow(): DataStream[OUT]

  /**
   * Writes the transformed data stream to configured output sinks.
   *
   * @param out
   *   a transformed stream from transform()
   */
  def sink(out: DataStream[OUT]): Unit =
    config.getSinkNames.foreach(name => runner.toSink[OUT](out, name))

  /**
   * The output stream will only be passed to BaseFlinkJob.sink if
   * FlinkConfig.mockEdges is false (ie, you're not testing).
   *
   * @param out
   *   the output data stream to pass into BaseFlinkJob.sink)
   */
  def maybeSink(out: DataStream[OUT]): Unit =
    if (!config.mockEdges) sink(out)

  /**
   * Runs the job, meaning it constructs the flow and executes it.
   *
   * @param limitOpt
   *   optional number of output events to return, for testing
   * @return
   *   either a list of output events or the job execution result
   */
  def run(limitOpt: Option[Int] = None)
      : Either[List[OUT], JobExecutionResult] = {

    logger.info(
      s"\nSTARTING FLINK JOB: ${config.jobName} ${config.jobArgs.mkString(" ")}\n"
    )

    val stream = flow()

    if (config.showPlan)
      logger.info(s"PLAN:\n${env.getExecutionPlan}\n")

    if (config.mockEdges) {
      val limit = limitOpt.getOrElse(
        Try(config.getJobConfig(config.jobName).getInt("run.limit"))
          .getOrElse(100)
      )
      Left(stream.executeAndCollect(config.jobName, limit))
    } else
      Right(env.execute(config.jobName))
  }
}
