package io.epiphanous.flinkrunner.flink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model.aggregate.{
  Aggregate,
  AggregateAccumulator,
  WindowedAggregationInitializer
}
import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkEvent}
import io.epiphanous.flinkrunner.util.StreamUtils.Pipe
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.util.Collector
import squants.Quantity

/** A streaming job. Implementers must provide a transform method,
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
      name: String = runner.getDefaultSourceName): DataStream[IN] =
    runner.configToSource[IN](runner.getSourceConfig(name))

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

  /** Create a data stream of windowed aggregates of type PWF_OUT. This
    * output stream will be instances of the ADT to be written to a sink.
    * @param source
    *   a keyed stream of events of type E
    * @param initializer
    *   a windowed aggregation initializer
    * @tparam E
    *   the input event type
    * @tparam KEY
    *   the type the stream is keyed on
    * @tparam WINDOW
    *   the window assigner type
    * @tparam AGG
    *   the aggregation type
    * @tparam QUANTITY
    *   the type of quantity being aggregate
    * @tparam PWF_OUT
    *   the type of output collected from the process window function in
    *   the initializer (should be a subclass of flinkrunner ADT)
    * @return
    */
  def windowedAggregation[
      E <: ADT: TypeInformation,
      KEY: TypeInformation,
      WINDOW <: Window: TypeInformation,
      AGG <: Aggregate: TypeInformation,
      QUANTITY <: Quantity[QUANTITY]: TypeInformation,
      PWF_OUT <: ADT: TypeInformation](
      source: KeyedStream[E, KEY],
      initializer: WindowedAggregationInitializer[
        E,
        KEY,
        WINDOW,
        AGG,
        QUANTITY,
        PWF_OUT,
        ADT
      ]): DataStream[PWF_OUT] = {

    implicit val accumulatorTypeInfo
        : TypeInformation[AggregateAccumulator[AGG]] =
      createTypeInformation[AggregateAccumulator[AGG]]

    source
      .window(initializer.windowAssigner)
      .allowedLateness(Time.seconds(initializer.allowedLateness.toSeconds))
      .aggregate(
        initializer.aggregateFunction,
        initializer.processWindowFunction
      )
  }

  /** Writes the transformed data stream to configured output sinks.
    *
    * @param out
    *   a transformed stream from transform()
    */
  def sink(out: DataStream[OUT]): Unit =
    runner.getSinkNames.foreach(name => runner.toSink[OUT](out, name))

  /** The output stream will only be passed to BaseFlinkJob.sink if
    * FlinkConfig.mockEdges is false (ie, you're not testing).
    *
    * @param out
    *   the output data stream to pass into BaseFlinkJob.sink)
    */
  def maybeSink(out: DataStream[OUT]): Unit =
    if (!runner.mockEdges) sink(out)

  /** Runs the job, meaning it constructs the flow and executes it.
    */
  def run(): Unit = {

    logger.info(
      s"\nSTARTING FLINK JOB: ${config.jobName} ${config.jobArgs.mkString(" ")}\n"
    )

    // build the job graph
    val stream = transform |# maybeSink

    if (config.showPlan)
      logger.info(s"\nPLAN:\n${env.getExecutionPlan}\n")

    runner.checkResultsOpt match {

      case Some(checkResults) =>
        logger.info(
          s"routing job ${config.jobName} results back through CheckResults<${checkResults.name}>"
        )
        val limit = config.getIntOpt("run.limit").getOrElse(100)
        checkResults.checkOutputEvents[OUT](
          stream.executeAndCollect(config.jobName, limit)
        )

      case _ =>
        val result = env.execute(config.jobName)
        logger.info(result.toString)
    }
  }
}
