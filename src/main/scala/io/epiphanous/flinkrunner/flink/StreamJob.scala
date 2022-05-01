package io.epiphanous.flinkrunner.flink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkEvent}
import io.epiphanous.flinkrunner.util.StreamUtils.Pipe
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.util.Collector

/**
 * A streaming job. Implementers must provide a transform method,
 * responsible for transforming inputs into outputs.
 * @param runner
 *   an instance of [[FlinkRunner]]
 * @tparam OUT
 *   the output type
 * @tparam ADT
 *   the algebraic data type of the [[FlinkRunner]] instance
 */
abstract class StreamJob[
    OUT: TypeInformation,
    ADT <: FlinkEvent: TypeInformation](runner: FlinkRunner[ADT])
    extends LazyLogging {

  val config: FlinkConfig              = runner.config
  val env: StreamExecutionEnvironment  = runner.env
  val tableEnv: StreamTableEnvironment = runner.tableEnv

  def transform: DataStream[OUT]

  def singleSource[IN <: ADT: TypeInformation](
      nameOpt: Option[String] = None): DataStream[IN] =
    runner.fromSource[IN](nameOpt)

  def connectedSource[
      IN1 <: ADT: TypeInformation,
      IN2 <: ADT: TypeInformation](
      source1Name: String,
      source2Name: String): ConnectedStreams[IN1, IN2] = {
    val source1 = singleSource[IN1](Some(source1Name))
    val source2 = singleSource[IN2](Some(source2Name))
    source1.connect(source2).keyBy[String](_.$key, _.$key)
  }

  def filterByControlSource[
      CONTROL <: ADT: TypeInformation,
      DATA <: ADT: TypeInformation](
      controlName: String,
      dataName: String): DataStream[DATA] = {
    val controlLockoutDuration =
      config.getDuration("control.lockout.duration").toMillis

    connectedSource[CONTROL, DATA](
      controlName,
      dataName
    ).map[Either[CONTROL, DATA]](
      (c: CONTROL) => Left(c),
      (d: DATA) => Right(d)
    ).keyBy[String]((cd: Either[CONTROL, DATA]) => cd.fold(_.$key, _.$key))
      .filterWithState[(Long, Boolean)] { case (cd, lastControlOpt) =>
        cd match {
          case Left(control) =>
            (
              false,
              if (
                lastControlOpt.forall { case (_, active) =>
                  control.$active != active
                }
              ) Some(control.$timestamp, control.$active)
              else lastControlOpt
            )
          case Right(data)   =>
            (
              lastControlOpt.exists { case (ts, active) =>
                active && ((data.$timestamp - ts) >= controlLockoutDuration)
              },
              lastControlOpt
            )
        }
      }
      .flatMap[DATA](
        (cd: Either[CONTROL, DATA], collector: Collector[DATA]) =>
          cd.foreach(d => collector.collect(d))
      )
  }

  def broadcastConnectedSource[
      IN <: ADT: TypeInformation,
      BC <: ADT: TypeInformation](
      keyedSourceName: String,
      broadcastSourceName: String): BroadcastConnectedStream[IN, BC] = {
    val keyedSource     =
      singleSource[IN](Some(keyedSourceName)).keyBy((in: IN) => in.$key)
    val broadcastSource =
      singleSource[BC](Some(broadcastSourceName)).broadcast(
        new MapStateDescriptor[String, BC](
          s"$keyedSourceName-$broadcastSourceName-state",
          createTypeInformation[String],
          createTypeInformation[BC]
        )
      )
    keyedSource.connect(broadcastSource)
  }

  /**
   * Writes the transformed data stream to configured output sinks.
   *
   * @param out
   *   a transformed stream from transform()
   */
  def sink(out: DataStream[OUT]): Unit =
    config.getSinkNames.foreach(name =>
      runner.toSink[OUT](out, Some(name))
    )

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

    val stream = transform |# maybeSink

    if (config.showPlan)
      logger.info(s"PLAN:\n${env.getExecutionPlan}\n")

    if (config.mockEdges) {
      val limit = limitOpt.getOrElse(
        config.getIntOpt("run.limit").getOrElse(100)
      )
      Left(stream.executeAndCollect(config.jobName, limit))
    } else
      Right(env.execute(config.jobName))
  }
}
