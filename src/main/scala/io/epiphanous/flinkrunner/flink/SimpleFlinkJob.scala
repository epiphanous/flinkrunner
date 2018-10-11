package io.epiphanous.flinkrunner.flink

//import com.fasterxml.jackson.databind.node.ObjectNode
import io.epiphanous.flinkrunner.model.FlinkEvent
import io.epiphanous.flinkrunner.util.StreamUtils._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream

/**
  * When a flink job needs to do a simple transformation on a single stream, such as
  * filtering, mapping, or windowing, [[SimpleFlinkJob]] can be helpful for simplifying
  * the implementation. It also helps job to be unit testable by separating load, transform,
  * and save steps.
  *
  * @tparam IN  A data type of input DataStream
  * @tparam OUT A data type of transformed/output DataStream
  * @param sources for testing, data topic names mapped to sequences of byte arrays
  */
abstract class SimpleFlinkJob[IN <: FlinkEvent: TypeInformation, OUT <: FlinkEvent: TypeInformation](
    sources: Map[String, Seq[Array[Byte]]] = Map.empty)
    extends FlinkJob[OUT](sources) {

  /**
    * Returns source data stream to pass into [[transform()]]. This must be overridden by subclasses.
    *
    * In general, code for getting a stream would be simple as follows:
    * {{{
    *   fromKafka()
    * }}}
    *
    * @return input data stream
    */
  def source()(implicit args: Args, env: SEE): DataStream[IN]

  /**
    * Primary method to transform the source data stream into the output data stream. The output of
    * this method is passed into [[sink()]]. This method must be overridden by subclasses.
    *
    * @param in input data stream created by [[source()]]
    * @param args implicit flink job args
    * @param env implicit flink execution environment
    * @return output data stream
    */
  def transform(in: DataStream[IN])(implicit args: Args, env: SEE): DataStream[OUT]

  /**
    * In order to write the transformed data stream to output sinks.
    *
    * In general, code for outputting a stream would be simple as follows:
    * {{{
    *   out.toKafka()
    *   out.toJdbc()
    * }}}
    * which writes the output to a configured kafka topic and jdbc table.
    *
    * @param out a transformed stream from [[transform()]]
    * @param args implicit flink job args
    * @param env implicit flink execution environment
    */
  def sink(out: DataStream[OUT])(implicit args: Args, env: SEE): Unit = {}

  /**
    * The output stream will only be passed to [[sink()]] if [[FlinkJobArgs.mockSink]] evaluates
    * to false (ie, you're not testing).
    *
    * @param out the output data stream to pass into [[sink()]]
    * @param args implicit flink job args
    * @param env implicit flink execution environment
    */
  def maybeSink(out: DataStream[OUT])(implicit args: Args, env: SEE): Unit =
    if (!args.mockSink) sink(out)

  /**
    * A pipeline for transforming a single stream. Passes the output of [[source()]]
    * through [[transform()]] and the result of that into [[maybeSink()]], which may pass it
    * into [[sink()]] if we're not testing. Ultimately, returns the output data stream to
    * facilitate testing.
    *
    * @return data output stream
    */
  override def flow(implicit args: Args, env: SEE): DataStream[OUT] =
    source |> transform |# maybeSink

}
