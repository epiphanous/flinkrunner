package io.epiphanous.flinkrunner.flink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.FlinkEvent
import io.epiphanous.flinkrunner.util.StreamUtils._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.JavaConverters._

abstract class FlinkJob[OUT <: FlinkEvent: TypeInformation](val sources: Map[String, Seq[Array[Byte]]] = Map.empty)
    extends LazyLogging {

  def flow(implicit args: Args, env: SEE): DataStream[OUT]

  def run(jobName: String, args: Array[String], extraArgs: Set[FlinkArgDef]): Either[Iterator[OUT], Unit] = {

    logger.info(s"\nSTARTING FLINK JOB: $jobName ${args.mkString(" ")}\n")

    implicit val jobArgs: FlinkJobArgs = new FlinkJobArgs(jobName, args, extraArgs)
    implicit val env: StreamExecutionEnvironment = configureEnvironment

    val stream = flow

    if (jobArgs.showPlan) logger.info(s"PLAN:\n${env.getExecutionPlan}\n")

    if (jobArgs.mockSink) {
      Left(DataStreamUtils.collect(stream.javaStream).asScala)
    } else
      Right(env.execute(jobName))
  }

}

trait FlinkJobObject {
  def jobDescription: String
  def extraArgs: Set[FlinkArgDef] = Set.empty
}
