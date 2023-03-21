package io.epiphanous.flinkrunner.flink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.model.aggregate.{
  Aggregate,
  AggregateAccumulator,
  WindowedAggregationInitializer
}
import io.epiphanous.flinkrunner.util.StreamUtils.Pipe
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.table.data.RowData
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
    extends LazyLogging
    with Serializable {

  val config: FlinkConfig = runner.config

  def transform: DataStream[OUT]

  def seqOrSingleSource[IN <: ADT: TypeInformation](
      seq: Seq[IN] = Seq.empty,
      name: Option[String] = None): DataStream[IN] =
    if (seq.nonEmpty) runner.env.fromCollection(seq)
    else singleSource[IN](name.getOrElse(runner.getDefaultSourceName))

  def seqOrSingleAvroSource[
      IN <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      seq: Seq[IN] = Seq.empty,
      name: Option[String] = None)(implicit
      fromKV: EmbeddedAvroRecordInfo[A] => IN): DataStream[IN] =
    if (seq.nonEmpty) runner.env.fromCollection[IN](seq)
    else
      singleAvroSource[IN, A](name.getOrElse(runner.getDefaultSourceName))

  def seqOrSingleRowSource[
      IN <: ADT with EmbeddedRowType: TypeInformation](
      seq: Seq[IN] = Seq.empty,
      name: Option[String] = None)(implicit
      fromRowData: RowData => IN): DataStream[IN] = if (seq.nonEmpty)
    runner.env.fromCollection[IN](seq)
  else singleRowSource[IN](name.getOrElse(runner.getDefaultSourceName))

  /** Configure a single input source stream.
    * @param name
    *   name of the configured source
    * @tparam IN
    *   the type of elements in the source stream
    * @return
    *   DataStream[IN]
    */
  def singleSource[IN <: ADT: TypeInformation](
      name: String = runner.getDefaultSourceName): DataStream[IN] =
    runner.configToSource[IN](runner.getSourceConfig(name))

  /** Configure a single avro source stream.
    *
    * @param name
    *   configured name of the source
    * @param fromKV
    *   an implicit method to construct an IN instance from an
    *   EmbeddedAvroRecordInfo[INA] instance. This is usually provided by
    *   having the companion class of your IN type extend
    *   EmbeddedAvroRecordFactory[INA].
    * @tparam IN
    *   the type of the input data stream element, which should extend
    *   EmbeddedAvroRecord[INA]
    * @tparam INA
    *   the type of the avro embedded record contained in the input data
    *   stream element
    * @return
    *   DataStream[IN]
    */
  def singleAvroSource[
      IN <: ADT with EmbeddedAvroRecord[INA]: TypeInformation,
      INA <: GenericRecord: TypeInformation](
      name: String = runner.getDefaultSourceName)(implicit
      fromKV: EmbeddedAvroRecordInfo[INA] => IN): DataStream[IN] =
    runner.configToAvroSource[IN, INA](runner.getSourceConfig(name))

  /** Configure a single input row source into a stream of elements of type
    * IN.
    *
    * @param name
    *   name of the configured source
    * @param fromRow
    *   implicit method to convert a Row to event of type E
    * @tparam IN
    *   the type of elements in the source stream
    * @return
    *   DataStream[IN]
    */
  def singleRowSource[IN <: ADT with EmbeddedRowType: TypeInformation](
      name: String = runner.getDefaultSourceName)(implicit
      fromRowData: RowData => IN): DataStream[IN] =
    runner.configToRowSource[IN](runner.getSourceConfig(name))

  /** Creates a connected data stream that joins two individual stream by a
    * joining key.
    * @param source1
    *   the first configured source
    * @param source2
    *   the second configured source
    * @param fun1
    *   a function to extract the joining key from elements in the first
    *   source stream
    * @param fun2
    *   a function to extract the joining key from elements in the second
    *   source stream
    * @tparam IN1
    *   the type of elements in the first source stream
    * @tparam IN2
    *   the type of elements in the second source stream
    * @tparam KEY
    *   the type of the joining key
    * @return
    *   ConnectedStreams[IN1,IN2]
    */
  def connectedSource[
      IN1 <: ADT: TypeInformation,
      IN2 <: ADT: TypeInformation,
      KEY: TypeInformation](
      source1: DataStream[IN1],
      source2: DataStream[IN2],
      fun1: IN1 => KEY,
      fun2: IN2 => KEY): ConnectedStreams[IN1, IN2] =
    source1.connect(source2).keyBy[KEY](fun1, fun2)

  /** A specialized connected source that combines a control stream with a
    * data stream. The control stream indicates when the data stream should
    * be considered active (by the control element's `\$active` method).
    * When the control stream indicates the data stream is active, data
    * elements are emitted. Otherwise, data elements are ignored. The
    * result is a stream of active data elements filtered dynamically by
    * the control stream.
    * @param controlSource
    *   the configured control stream
    * @param dataSource
    *   the configured data stream
    * @param fun1
    *   a function to compute a joining key from elements in the control
    *   stream
    * @param fun2
    *   a function to compute a joining key from elements in the data
    *   stream
    * @tparam CONTROL
    *   the type of elements in the control stream
    * @tparam DATA
    *   the type of elements in the data stream
    * @tparam KEY
    *   the type of the joining key
    * @return
    *   DataStream[DATA]
    */
  def filterByControlSource[
      CONTROL <: ADT: TypeInformation,
      DATA <: ADT: TypeInformation,
      KEY: TypeInformation](
      controlSource: DataStream[CONTROL],
      dataSource: DataStream[DATA],
      fun1: CONTROL => KEY,
      fun2: DATA => KEY): DataStream[DATA] = {
    val controlLockoutDuration =
      config
        .getDurationOpt("control.lockout.duration")
        .map(_.toMillis)
        .getOrElse(0L)

    connectedSource[CONTROL, DATA, KEY](
      controlSource,
      dataSource,
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
              false, // controls are never emitted
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

  /** Creates a specialized connected source stream that joins a keyed data
    * stream with a broadcast stream. Elements in the broadcast stream are
    * connected to each of the keyed streams and can be processed with a
    * special CoProcessFunction, as described in Flink's documentation:
    *
    * https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/broadcast_state/.
    *
    * @param keyedSource
    *   the configured keyed source
    * @param broadcastSource
    *   the configured broadcast source
    * @tparam IN
    *   the type of elements in the keyed source stream
    * @tparam BC
    *   the type of elements in the broadcast source stream
    * @tparam KEY
    *   the type of the key of the keyed source
    * @return
    *   BroadcastConnectedStream[IN,BC]
    */
  def broadcastConnectedSource[
      IN <: ADT: TypeInformation,
      BC <: ADT: TypeInformation,
      KEY: TypeInformation](
      keyedSource: DataStream[IN],
      broadcastSource: DataStream[BC]): BroadcastConnectedStream[IN, BC] =
    keyedSource.connect(
      broadcastSource.broadcast(
        new MapStateDescriptor[KEY, BC](
          s"${keyedSource.name}-${broadcastSource.name}-state",
          createTypeInformation[KEY],
          createTypeInformation[BC]
        )
      )
    )

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
    runner.getSinkNames.foreach(name => runner.addSink[OUT](out, name))

  /** The output stream will only be passed to output sink(s) if the runner
    * determines it's required. Some testing configurations can skip
    * actually writing to the sink if they are only testing the
    * transformation logic of a job.
    *
    * @param out
    *   the output data stream to pass to the sink method
    */
  def maybeSink(out: DataStream[OUT]): Unit =
    if (runner.writeToSink) sink(out)

  /** Runs the job, meaning it constructs the flow and executes it.
    */
  def run(): Unit = {

    logger.info(
      s"\nSTARTING FLINK JOB: ${config.jobName} ${config.jobArgs.mkString(" ")}\n"
    )

    // build the job graph
    val stream = transform |# maybeSink

    if (config.showPlan)
      logger.info(s"\nPLAN:\n${runner.getExecutionPlan}\n")

    runner.checkResultsOpt match {

      case Some(checkResults) =>
        logger.info(
          s"routing job ${config.jobName} results back through CheckResults<${checkResults.name}>"
        )
        checkResults.checkOutputEvents[OUT](
          stream.executeAndCollect(
            config.jobName,
            checkResults.collectLimit
          )
        )

      case _ =>
        val result = runner.execute
        logger.info(result.toString)
    }
  }
}
