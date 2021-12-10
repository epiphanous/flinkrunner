package io.epiphanous.flinkrunner.flink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkEvent}
import io.epiphanous.flinkrunner.util.StreamUtils.Pipe
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{
  DataStream,
  StreamExecutionEnvironment
}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import scala.util.Try

/**
 * An abstract flink job to transform on an input stream into an output
 * stream.
 * @param runner
 *   the flink runner associated with this job
 * @tparam DS
 *   The type of the input data stream (not its elements)
 * @tparam OUT
 *   The type of output stream elements
 * @tparam ADT
 *   The flink runner's algebraic data type
 */
abstract class BaseFlinkJob[
    DS: TypeInformation,
    OUT <: ADT: TypeInformation,
    ADT <: FlinkEvent: TypeInformation](runner: FlinkRunner[ADT])
    extends LazyLogging {

  val config: FlinkConfig[ADT]         = runner.config
  val env: StreamExecutionEnvironment  = runner.env
  val tableEnv: StreamTableEnvironment = runner.tableEnv

  /**
   * A pipeline for transforming a single stream. Passes the output of
   * source() through transform() and the result of that into maybeSink(),
   * which may pass it into sink() if we're not testing. Ultimately,
   * returns the output data stream to facilitate testing.
   *
   * @return
   *   data output stream
   */
  def flow(): DataStream[OUT] = source |> transform |# maybeSink

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

  /**
   * Returns source data stream to pass into transform(). This must be
   * overridden by subclasses.
   *
   * @return
   *   input data stream
   */
  def source(): DS

  /**
   * Primary method to transform the source data stream into the output
   * data stream. The output of this method is passed into sink(). This
   * method must be overridden by subclasses.
   *
   * @param in
   *   input data stream created by source()
   * @return
   *   output data stream
   */
  def transform(in: DS): DataStream[OUT]

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

}
