package io.epiphanous.flinkrunner.flink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.SEE
import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkEvent}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.JavaConverters._

abstract class BaseFlinkJob[OUT <: FlinkEvent: TypeInformation] extends LazyLogging {

  def flow(implicit config: FlinkConfig, env: SEE): DataStream[OUT]

  def run(implicit config: FlinkConfig, env: SEE): Either[Iterator[OUT], Unit] = {

    logger.info(s"\nSTARTING FLINK JOB: $config.jobName ${config.jobArgs}\n")

    val stream = flow

    if (config.showPlan) logger.info(s"PLAN:\n${env.getExecutionPlan}\n")

    if (config.mockEdges) {
      Left(DataStreamUtils.collect(stream.javaStream).asScala)
    } else
      Right(env.execute(config.jobName))
  }

}
