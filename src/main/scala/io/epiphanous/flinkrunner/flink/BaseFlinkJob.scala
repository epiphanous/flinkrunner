package io.epiphanous.flinkrunner.flink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.SEE
import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkEvent}
import io.epiphanous.flinkrunner.util.StreamUtils._
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.scala.DataStream

import scala.collection.JavaConverters._

/**
  * An abstract flink job to transform on an input stream into an output stream.
  *
  * @tparam DS   The type of the input stream
  * @tparam OUT  The type of output stream elements
  */
abstract class BaseFlinkJob[DS, OUT <: FlinkEvent: TypeInformation] extends LazyLogging {

  /**
    * A pipeline for transforming a single stream. Passes the output of [[source()]]
    * through [[transform()]] and the result of that into [[maybeSink()]], which may pass it
    * into [[sink()]] if we're not testing. Ultimately, returns the output data stream to
    * facilitate testing.
    *
    * @param config implicit flink job config
    * @return data output stream
    */
  def flow()(implicit config: FlinkConfig, env: SEE): DataStream[OUT] =
    source |> transform |# maybeSink

  def run()(implicit config: FlinkConfig, env: SEE): Either[Iterator[OUT], JobExecutionResult] = {

    logger.info(s"\nSTARTING FLINK JOB: ${config.jobName} ${config.jobArgs.mkString(" ")}\n")

    val stream = flow

    if (config.showPlan) logger.info(s"PLAN:\n${env.getExecutionPlan}\n")

    if (config.mockEdges)
      Left(DataStreamUtils.collect(stream.javaStream).asScala)
    else
      Right(env.execute(config.jobName))
  }

  /**
    * Returns source data stream to pass into [[transform()]]. This must be overridden by subclasses.
    *
    * @return input data stream
    */
  def source()(implicit config: FlinkConfig, env: SEE): DS

  /**
    * Primary method to transform the source data stream into the output data stream. The output of
    * this method is passed into [[sink()]]. This method must be overridden by subclasses.
    *
    * @param in     input data stream created by [[source()]]
    * @param config implicit flink job config
    * @return output data stream
    */
  def transform(in: DS)(implicit config: FlinkConfig, env: SEE): DataStream[OUT]

  /**
    * Writes the transformed data stream to configured output sinks.
    *
    * @param out a transformed stream from [[transform()]]
    * @param config implicit flink job config
    */
  def sink(out: DataStream[OUT])(implicit config: FlinkConfig, env: SEE): Unit =
    config.getSinkNames.foreach(name => out.toSink(name))

  /**
    * The output stream will only be passed to [[sink()]]
    * if [[FlinkConfig.mockEdges]] is false (ie, you're not testing).
    *
    * @param out the output data stream to pass into [[sink()]]
    * @param config implicit flink job config
    */
  def maybeSink(out: DataStream[OUT])(implicit config: FlinkConfig, env: SEE): Unit =
    if (!config.mockEdges) sink(out)

}
