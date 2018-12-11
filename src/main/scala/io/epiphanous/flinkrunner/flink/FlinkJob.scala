package io.epiphanous.flinkrunner.flink

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.SEE
import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkEvent}
import io.epiphanous.flinkrunner.util.StreamUtils._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.scala.DataStream

import scala.collection.JavaConverters._

/**
  * An abstract flink job to transform on a stream of events from an algebraic data type (ADT).
  *
  * @tparam IN   The type of input stream elements
  * @tparam OUT  The type of output stream elements
  */
abstract class FlinkJob[IN <: FlinkEvent: TypeInformation, OUT <: FlinkEvent: TypeInformation]
    extends BaseFlinkJob[OUT] {

  /**
    * Returns source data stream to pass into [[transform()]]. This can be overridden by subclasses.
    * @return input data stream
    */
  def source()(implicit config: FlinkConfig, env: SEE): DataStream[IN] =
    fromSource[IN](config.getSourceNames.head) |# maybeAssignTimestampsAndWatermarks

  def maybeAssignTimestampsAndWatermarks(in: DataStream[IN])(implicit config: FlinkConfig, env: SEE): Unit =
    if (env.getStreamTimeCharacteristic == TimeCharacteristic.EventTime)
      in.assignTimestampsAndWatermarks(boundedLatenessEventTime[IN]())

  /**
    * Primary method to transform the source data stream into the output data stream. The output of
    * this method is passed into [[sink()]]. This method must be overridden by subclasses.
    *
    * @param in input data stream created by [[source()]]
    * @param config implicit flink job config
    * @return output data stream
    */
  def transform(in: DataStream[IN])(implicit config: FlinkConfig): DataStream[OUT]

  /**
    * Writes the transformed data stream to configured output sinks.
    **
    * @param out a transformed stream from [[transform()]]
    * @param config implicit flink job config
    */
  def sink(out: DataStream[OUT])(implicit config: FlinkConfig): Unit =
    config.getSinkNames.foreach(name => out.toSink(name))

  /**
    * The output stream will only be passed to [[sink()]] if [[FlinkConfig.mockEdges]] evaluates
    * to false (ie, you're not testing).
    *
    * @param out the output data stream to pass into [[sink()]]
    * @param config implicit flink job config
    */
  def maybeSink(out: DataStream[OUT])(implicit config: FlinkConfig): Unit =
    if (!config.mockEdges) sink(out)

  /**
    * A pipeline for transforming a single stream. Passes the output of [[source()]]
    * through [[transform()]] and the result of that into [[maybeSink()]], which may pass it
    * into [[sink()]] if we're not testing. Ultimately, returns the output data stream to
    * facilitate testing.
    *
    * @param config implicit flink job config
    * @return data output stream
    */
  def flow(implicit config: FlinkConfig, env: SEE): DataStream[OUT] =
    source |> transform |# maybeSink

}
