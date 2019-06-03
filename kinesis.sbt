
val withKinesis = """
package io.epiphanous.flinkrunner.util
import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.SEE
import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkEvent, KinesisSinkConfig, KinesisSourceConfig}
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kinesis.{FlinkKinesisConsumer, FlinkKinesisProducer}

object KinesisStreamUtils extends LazyLogging {

  /**
    * Configure stream from kinesis.
    * @param srcConfig a source config
    * @param config implicitly provided job config
    * @tparam E stream element type
    * @return DataStream[E]
    */
  def fromKinesis[E <: FlinkEvent: TypeInformation](
    srcConfig: KinesisSourceConfig
  )(implicit config: FlinkConfig,
    env: SEE
  ): DataStream[E] = {
    val consumer =
      new FlinkKinesisConsumer[E](srcConfig.stream,
                                  config.getDeserializationSchema.asInstanceOf[DeserializationSchema[E]],
                                  srcConfig.properties)
    env
      .addSource(consumer)
      .name(srcConfig.label)
  }

  /**
    * Send stream to a kinesis sink.
    * @param stream the data stream
    * @param sinkConfig a sink configuration
    * @param config implicit job args
    * @tparam E stream element type
    * @return DataStreamSink[E]
    */
  def toKinesis[E <: FlinkEvent: TypeInformation](
    stream: DataStream[E],
    sinkConfig: KinesisSinkConfig
  )(implicit config: FlinkConfig
  ): DataStreamSink[E] =
    stream
      .addSink({
        val sink =
          new FlinkKinesisProducer[E](config.getSerializationSchema.asInstanceOf[SerializationSchema[E]],
                                      sinkConfig.properties)
        sink.setDefaultStream(sinkConfig.stream)
        sink.setFailOnError(true)
        sink.setDefaultPartition("0")
        sink
      })
      .name(sinkConfig.label)
}
"""

val withoutKinesis = """
package io.epiphanous.flinkrunner.util
import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.SEE
import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkEvent, KinesisSinkConfig, KinesisSourceConfig}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.DataStream

object KinesisStreamUtils extends LazyLogging {

  def complain = {
    val message =
      "This version of flinkrunner does not support AWS Kinesis due to licensing restrictions.\n"+
      "You can build flinkrunner with kinesis support yourself. See https://github.com/epiphanous/flinkrunner/README.md#kinesis."
    logger.error(message)
    throw new UnsupportedOperationException(message)
  }

  /**
    * Throws error since kinesis not supported in this version of flinkrunner.
    * @param srcConfig a source config
    * @param config implicitly provided job config
    * @tparam E stream element type
    * @return DataStream[E]
    */
  def fromKinesis[E <: FlinkEvent: TypeInformation](
    srcConfig: KinesisSourceConfig
  ): DataStream[E] = complain

  /**
    * Throws error since kinesis not supported in this version of flinkrunner.
    * @param stream the data stream
    * @param sinkConfig a sink configuration
    * @param config implicit job args
    * @tparam E stream element type
    * @return DataStreamSink[E]
    */
  def toKinesis[E <: FlinkEvent: TypeInformation](
    stream: DataStream[E],
    sinkConfig: KinesisSinkConfig
  ): DataStreamSink[E] = complain
}
"""

sourceGenerators in Compile += Def.task {
  val file = (sourceManaged in Compile).value / "kinesis" / "io" / "epiphanous" / "flinkrunner" / "util" / "KinesisStreamUtils.scala"
  IO.write(file, System.getProperty("with.kinesis", "false") match {
    case "true" | "1" | "yes" =>
      println("*** compiling with kinesis support")
      withKinesis
    case _ =>
      println("*** compiling without kinesis support")
      withoutKinesis
  })
  Seq(file)
}
