package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.util.StreamUtils._
import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.JavaConverters._

abstract class FlinkJob(val sources: Map[String, Seq[Array[Byte]]] = Map.empty) extends LazyLogging {

  def flow(implicit args: Args, env: SEE): DataStream[_]

  def run(jobName: String, args: Array[String], extraArgs: Set[FlinkArgDef]): Either[Iterator[Any], Unit] = {

    logger.info(s"\nSTARTING FLINK JOB: $jobName ${args.mkString(" ")}\n")

    implicit val jobArgs: FlinkJobArgs           = new FlinkJobArgs(jobName, args, extraArgs)
    implicit val env: StreamExecutionEnvironment = configureEnvironment

    val stream = flow

    if (jobArgs.showPlan) logger.info(s"PLAN:\n${env.getExecutionPlan}\n")

    if (jobArgs.mockEdges) {
      Left(DataStreamUtils.collect(stream.javaStream).asScala)
    } else
      Right(env.execute(jobName))
  }

}

trait FlinkJobObject {
  def jobDescription: String
  def extraArgs: Set[FlinkArgDef] = Set.empty
}
