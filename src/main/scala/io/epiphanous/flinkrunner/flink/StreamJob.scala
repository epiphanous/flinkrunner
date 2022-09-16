package io.epiphanous.flinkrunner.flink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model.aggregate.{
  Aggregate,
  AggregateAccumulator,
  WindowedAggregationInitializer
}
import io.epiphanous.flinkrunner.model.{
  EmbeddedAvroRecord,
  EmbeddedAvroRecordInfo,
  FlinkConfig,
  FlinkEvent
}
import io.epiphanous.flinkrunner.util.StreamUtils.Pipe
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
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

  /** Creates a connected data stream that joins two individual stream by a
    * joining key.
    * @param source1Name
    *   name of the first configured source
    * @param source2Name
    *   name of the second configured source
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
      source1Name: String,
      source2Name: String,
      fun1: IN1 => KEY,
      fun2: IN2 => KEY): ConnectedStreams[IN1, IN2] = {
    val source1 = singleSource[IN1](source1Name)
    val source2 = singleSource[IN2](source2Name)
    source1.connect(source2).keyBy[KEY](fun1, fun2)
  }

  /** A specialized connected source that combines a control stream with a
    * data stream. The control stream indicates when the data stream should
    * be considered active (by the control element's $active method). When
    * the control stream indicates the data stream is active, data elements
    * are emitted. Otherwise, data elements are ignored. The result is a
    * stream of active data elements filtered dynamically by the control
    * stream.
    * @param controlName
    *   name of the configured control stream
    * @param dataName
    *   name of the configured data stream
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

  /** Creates a specialized connected source stream that joins a keyed data
    * stream with a broadcast stream. Elements in the broadcast stream are
    * connected to each of the keyed streams and can be processed with a
    * special CoProcessFunction, as described in Flink's documentation:
    *
    * https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/broadcast_state/.
    *
    * @param keyedSourceName
    *   name of the configured keyed source
    * @param broadcastSourceName
    *   name of the configured broadcast source
    * @param keyedSourceGetKeyFunc
    *   a function to extract a key from elements in the keyed source
    *   stream
    * @tparam IN
    *   the type of elements in the keyed source stream
    * @tparam BC
    *   the type of elements in the broadcast source stream
    * @tparam KEY
    *   the type of the key
    * @return
    *   BroadcastConnectedStream[IN,BC]
    */
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

  /** Configure a single avro source stream.
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

  /** Create a connected data stream joining two avro source streams by a
    * joining key.
    * @param source1Name
    *   the first configured avro source
    * @param source2Name
    *   the second configured avro source
    * @param in1GetKeyFunc
    *   a function to get the key from first source elements
    * @param in2GetKeyFunc
    *   a function to the key from second source elements
    * @param fromKV1
    *   an implicit method to construct an IN1 instance from an
    *   EmbeddedAvroRecordInfo[IN1A] instance. This is usually provided by
    *   having the companion class of your IN1 type extend
    *   EmbeddedAvroRecordFactory[IN1A].
    * @param fromKV2
    *   an implicit method to construct an IN1 instance from an
    *   EmbeddedAvroRecordInfo[IN1A] instance. This is usually provided by
    *   having the companion class of your IN1 type extend
    *   EmbeddedAvroRecordFactory[IN1A].
    * @tparam IN1
    *   the type of elements in the first data stream
    * @tparam IN1A
    *   the avro type embedded within elements of the first data stream
    * @tparam IN2
    *   the type of elements in the second data stream
    * @tparam IN2A
    *   the avro type embedded within elements of the second data stream
    * @tparam KEY
    *   the type of the joining key
    * @return
    *   ConnectedStreams[IN1,IN2]
    */
  def connectedAvroSource[
      IN1 <: ADT with EmbeddedAvroRecord[IN1A]: TypeInformation,
      IN1A <: GenericRecord: TypeInformation,
      IN2 <: ADT with EmbeddedAvroRecord[IN2A]: TypeInformation,
      IN2A <: GenericRecord: TypeInformation,
      KEY: TypeInformation](
      source1Name: String,
      source2Name: String,
      in1GetKeyFunc: IN1 => KEY,
      in2GetKeyFunc: IN2 => KEY)(implicit
      fromKV1: EmbeddedAvroRecordInfo[IN1A] => IN1,
      fromKV2: EmbeddedAvroRecordInfo[IN2A] => IN2
  ): ConnectedStreams[IN1, IN2] = {
    val source1 = singleAvroSource[IN1, IN1A](source1Name)
    val source2 = singleAvroSource[IN2, IN2A](source2Name)
    source1.connect(source2).keyBy[KEY](in1GetKeyFunc, in2GetKeyFunc)
  }

  /** A specialized connected avro source that combines an avro control
    * stream with an avro data stream. The control stream indicates when
    * the data stream should be considered active (by the control element's
    * $active method). When the control stream indicates the data stream is
    * active, data elements are emitted. Otherwise, data elements are
    * ignored. The result is a stream of active data elements filtered
    * dynamically by the control stream.
    * @param controlName
    *   name of the configured avro control stream
    * @param dataName
    *   name of the configured avro data stream
    * @param controlGetKeyFunc
    *   a function to compute the joining key for the control stream
    * @param dataGetKeyFunc
    *   a function to compute the joining key for the data stream
    * @param fromKVControl
    *   an implicit method to construct a CONTROL instance from an
    *   EmbeddedAvroRecordInfo[CONTROLA] instance. This is usually provided
    *   by having the companion class of your CONTROL type extend
    *   EmbeddedAvroRecordFactory[CONTROLA].
    * @param fromKVData
    *   an implicit method to construct a DATA instance from an
    *   EmbeddedAvroRecordInfo[DATAA] instance. This is usually provided by
    *   having the companion class of your DATA type extend
    *   EmbeddedAvroRecordFactory[DATAA].
    * @tparam CONTROL
    *   the type of elements in the control stream
    * @tparam CONTROLA
    *   the type of avro record embedded within elements in the control
    *   stream
    * @tparam DATA
    *   the type of elements in the data stream
    * @tparam DATAA
    *   the type of avro record embedded within elements in the data stream
    * @tparam KEY
    *   the type of joining key
    * @return
    *   DataStream[DATA]
    */
  def filterByControlAvroSource[
      CONTROL <: ADT with EmbeddedAvroRecord[CONTROLA]: TypeInformation,
      CONTROLA <: GenericRecord: TypeInformation,
      DATA <: ADT with EmbeddedAvroRecord[DATAA]: TypeInformation,
      DATAA <: GenericRecord: TypeInformation,
      KEY: TypeInformation](
      controlName: String,
      dataName: String,
      controlGetKeyFunc: CONTROL => KEY,
      dataGetKeyFunc: DATA => KEY)(implicit
      fromKVControl: EmbeddedAvroRecordInfo[CONTROLA] => CONTROL,
      fromKVData: EmbeddedAvroRecordInfo[DATAA] => DATA
  ): DataStream[DATA] = {
    val controlLockoutDuration                                          =
      config.getDuration("control.lockout.duration").toMillis
    implicit val eitherTypeInfo: TypeInformation[Either[CONTROL, DATA]] =
      TypeInformation.of(new TypeHint[Either[CONTROL, DATA]] {})
    implicit val longBoolTypeInfo: TypeInformation[(Long, Boolean)]     =
      TypeInformation.of(new TypeHint[(Long, Boolean)] {})
    connectedAvroSource[CONTROL, CONTROLA, DATA, DATAA, KEY](
      controlName,
      dataName,
      controlGetKeyFunc,
      dataGetKeyFunc
    ).map[Either[CONTROL, DATA]](
      (c: CONTROL) => Left(c),
      (d: DATA) => Right(d)
    ).keyBy[KEY]((cd: Either[CONTROL, DATA]) =>
      cd.fold(controlGetKeyFunc, dataGetKeyFunc)
    ).filterWithState[(Long, Boolean)] { case (cd, lastControlOpt) =>
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
    }.flatMap[DATA](
      (cd: Either[CONTROL, DATA], collector: Collector[DATA]) =>
        cd.foreach(d => collector.collect(d))
    )
  }

  /** Creates a specialized connected avro source stream that joins a keyed
    * avro data stream with an avro broadcast stream. Elements in the
    * broadcast stream are connected to each of the keyed streams and can
    * be processed with a special CoProcessFunction, as described in
    * Flink's documentation:
    *
    * https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/broadcast_state/.
    *
    * @param keyedSourceName
    *   name of the configured keyed source
    * @param broadcastSourceName
    *   name of the configured broadcast source
    * @param keyedSourceGetKeyFunc
    *   a function to extract the key from elements of the keyed source
    *   stream
    * @param fromKVIN
    *   an implicit method to construct an IN instance from an
    *   EmbeddedAvroRecordInfo[INA] instance. This is usually provided by
    *   having the companion class of your IN type extend
    *   EmbeddedAvroRecordFactory[INA].
    * @param fromKVBC
    *   an implicit method to construct a BC instance from an
    *   EmbeddedAvroRecordInfo[BCA] instance. This is usually provided by
    *   having the companion class of your BC type extend
    *   EmbeddedAvroRecordFactory[BCA].
    * @tparam IN
    *   type type of elements in the keyed source stream
    * @tparam INA
    *   the type of avro record embedded in elements in the keyed source
    *   stream
    * @tparam BC
    *   the type of elements in the broadcast stream
    * @tparam BCA
    *   the type of avro record embedded in elements in the broadcast
    *   source stream
    * @tparam KEY
    *   the type of key in the keyed stream
    * @return
    *   BroadcastConnectedStream[IN,BC]
    */
  def broadcastConnectedAvroSource[
      IN <: ADT with EmbeddedAvroRecord[INA]: TypeInformation,
      INA <: GenericRecord: TypeInformation,
      BC <: ADT with EmbeddedAvroRecord[BCA]: TypeInformation,
      BCA <: GenericRecord: TypeInformation,
      KEY: TypeInformation](
      keyedSourceName: String,
      broadcastSourceName: String,
      keyedSourceGetKeyFunc: IN => KEY)(implicit
      fromKVIN: EmbeddedAvroRecordInfo[INA] => IN,
      fromKVBC: EmbeddedAvroRecordInfo[BCA] => BC)
      : BroadcastConnectedStream[IN, BC] = {
    val keyedSource     =
      singleAvroSource[IN, INA](keyedSourceName)
        .keyBy[KEY](keyedSourceGetKeyFunc)
    val broadcastSource =
      singleAvroSource[BC, BCA](broadcastSourceName).broadcast(
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
      logger.info(s"\nPLAN:\n${env.getExecutionPlan}\n")

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
        val result = env.execute(config.jobName)
        logger.info(result.toString)
    }
  }
}
