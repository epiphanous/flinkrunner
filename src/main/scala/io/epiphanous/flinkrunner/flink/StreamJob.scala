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
    OUT <: ADT: TypeInformation,
    ADT <: FlinkEvent: TypeInformation](runner: FlinkRunner[ADT])
    extends LazyLogging {

  val config: FlinkConfig              = runner.config
  val env: StreamExecutionEnvironment  = runner.env
  val tableEnv: StreamTableEnvironment = runner.tableEnv

  def transform: DataStream[OUT]

  def singleSource[IN <: ADT: TypeInformation](
      name: String): DataStream[IN] =
    runner.fromSource[IN](name)

  def connectedSource[
      IN1 <: ADT: TypeInformation,
      IN2 <: ADT: TypeInformation,
      KEY: TypeInformation](
      source1Name: String,
      source2Name: String,
      fun1: IN1 => KEY,
      fun2: IN2 => KEY): ConnectedStreams[IN1, IN2] = {
    val source1 = singleSource[IN1](source1Name)
    val source2 = singleSource[IN2](source2Name)
    source1.connect(source2).keyBy[KEY](fun1, fun2)
  }

  def filterByControlSource[
      CONTROL <: ADT: TypeInformation,
      DATA <: ADT: TypeInformation,
      KEY: TypeInformation](
      controlName: String,
      dataName: String,
      fun1: CONTROL => KEY,
      fun2: DATA => KEY): DataStream[DATA] = {
    val controlLockoutDuration =
      config.getDuration("control.lockout.duration").toMillis

    connectedSource[CONTROL, DATA, KEY](
      controlName,
      dataName,
      fun1,
      fun2
    ).map[Either[CONTROL, DATA]](
      (c: CONTROL) => Left(c),
      (d: DATA) => Right(d)
    ).keyBy[KEY]((cd: Either[CONTROL, DATA]) => cd.fold(fun1, fun2))
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
      BC <: ADT: TypeInformation,
      KEY: TypeInformation](
      keyedSourceName: String,
      broadcastSourceName: String,
      keyedSourceGetKeyFunc: IN => KEY)
      : BroadcastConnectedStream[IN, BC] = {
    val keyedSource     =
      singleSource[IN](keyedSourceName)
        .keyBy[KEY](keyedSourceGetKeyFunc)
    val broadcastSource =
      singleSource[BC](broadcastSourceName).broadcast(
        new MapStateDescriptor[KEY, BC](
          s"$keyedSourceName-$broadcastSourceName-state",
          createTypeInformation[KEY],
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

    // build the job graph
    val stream = transform |# maybeSink

    if (config.showPlan) {
      logger.info(s"\nPLAN:\n${env.getExecutionPlan}\n")
//      logger.info(
//        s"\nOUTPUT TABLE STRUCTURE:\n${tableEnv.fromDataStream(transform).getResolvedSchema}\n"
//      )
    }

    if (config.mockEdges) {
      val limit = limitOpt.getOrElse(
        config.getIntOpt("run.limit").getOrElse(100)
      )
      Left(stream.executeAndCollect(config.jobName, limit))
    } else
      Right(env.execute(config.jobName))
  }
}
