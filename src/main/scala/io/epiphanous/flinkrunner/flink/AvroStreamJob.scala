package io.epiphanous.flinkrunner.flink

import io.epiphanous.flinkrunner.FlinkRunner
import io.epiphanous.flinkrunner.model.{
  EmbeddedAvroRecord,
  EmbeddedAvroRecordInfo,
  FlinkEvent
}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.api.scala.{
  createTypeInformation,
  BroadcastConnectedStream,
  ConnectedStreams,
  DataStream
}
import org.apache.flink.util.Collector

/** A [[StreamJob]] with Avro inputs and outputs
  * @param runner
  *   an instance of [[FlinkRunner]]
  * @tparam OUT
  *   the output type, with an embedded avro record of type A
  * @tparam A
  *   the type of avro record that is embedded in our output type. only
  *   this avro part will be written to the sink.
  * @tparam ADT
  *   the algebraic data type of the [[FlinkRunner]] instance
  */
abstract class AvroStreamJob[
    OUT <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
    A <: GenericRecord: TypeInformation,
    ADT <: FlinkEvent: TypeInformation](runner: FlinkRunner[ADT])
    extends StreamJob[OUT, ADT](runner) {

  def singleAvroSource[
      IN <: ADT with EmbeddedAvroRecord[INA]: TypeInformation,
      INA <: GenericRecord: TypeInformation](
      name: String = runner.getDefaultSourceName)(implicit
      fromKV: EmbeddedAvroRecordInfo[INA] => IN): DataStream[IN] =
    runner.configToAvroSource[IN, INA](runner.getSourceConfig(name))

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
      fromKV2: EmbeddedAvroRecordInfo[IN2A] => IN2)
      : ConnectedStreams[IN1, IN2] = {
    val source1 = singleAvroSource[IN1, IN1A](source1Name)
    val source2 = singleAvroSource[IN2, IN2A](source2Name)
    source1.connect(source2).keyBy[KEY](in1GetKeyFunc, in2GetKeyFunc)
  }

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
      fromKVData: EmbeddedAvroRecordInfo[DATAA] => DATA)
      : DataStream[DATA] = {
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

  override def sink(out: DataStream[OUT]): Unit =
    runner.getSinkNames.foreach(name =>
      runner.toAvroSink[OUT, A](out, name)
    )
}
