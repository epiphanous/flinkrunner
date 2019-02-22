package io.epiphanous.flinkrunner.util

import java.io.{File, FileNotFoundException}
import java.nio.charset.StandardCharsets

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.SEE
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.operator.AddToJdbcBatchFunction
import org.apache.flink.api.common.serialization.{DeserializationSchema, Encoder, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.{
  BasePathBucketAssigner,
  DateTimeBucketAssigner
}
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.{
  DefaultRollingPolicy,
  OnCheckpointRollingPolicy
}
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor.IgnoringHandler
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisSerializationSchema
import org.apache.flink.streaming.connectors.kinesis.{FlinkKinesisConsumer, FlinkKinesisProducer}
import org.apache.flink.streaming.util.serialization.{KeyedDeserializationSchema, KeyedSerializationSchema}

object StreamUtils extends LazyLogging {

  val RESOURCE_PATTERN = "resource://(.*)".r

  /**
    * A little syntactic sugar for writing stream program. This is the pipe operator, ala F#.
    *
    * Assuming {{{source}}} This let's us write
    * {{{
    *   def program:DataStream[E] = source |> transform |# sink
    * }}}
    * instead of
    * {{{
    *   def program:DataStream[E] = {
    *     val result:DataStream[E] = transform(source)
    *     sink(result)
    *     result
    *   }
    * }}}
    *
    * @param v any object
    * @tparam A the type of v
    */
  implicit class Pipe[A](val v: A) extends AnyVal {
    // forward pipe op
    def |>[B](t: A => B) = t(v)

    // side effect op
    def |#(e: A => Unit): A = { e(v); v }
  }

  /**
    * Generates a timestamp and watermark assigner for a stream with a given type of element that limits
    * how late an element is allowed to arrive in event time.
    *
    * @param config implicitly provided job config
    * @tparam E the type of stream element
    * @return BoundedLatenessGenerator[E]
    */
  def boundedLatenessEventTime[E <: FlinkEvent: TypeInformation](
  )(implicit config: FlinkConfig
  ): BoundedLatenessGenerator[E] =
    new BoundedLatenessGenerator[E](config.maxLateness.toMillis)

  /**
    * Creates an ascending timestamp extractor.
    * @tparam E type of stream element
    * @return AscendingTimestampExtractor[E]
    */
  def ascendingTimestampExtractor[E <: FlinkEvent: TypeInformation](): AscendingTimestampExtractor[E] = {
    val extractor: AscendingTimestampExtractor[E] = new AscendingTimestampExtractor[E] {
      var lastTimestamp = Long.MinValue
      def extractAscendingTimestamp(event: E) = {
        lastTimestamp = event.$timestamp
        lastTimestamp
      }
    }
    extractor.withViolationHandler(new IgnoringHandler())
    extractor
  }

  /**
    * Configure stream source from configuration.
    * @param sourceName the name of the source to get its configuration
    * @tparam E stream element type
    * @return DataStream[E]
    */
  def fromSource[E <: FlinkEvent: TypeInformation](
    sourceName: String = ""
  )(implicit config: FlinkConfig,
    env: SEE
  ): DataStream[E] = {
    val name = if (sourceName.isEmpty) config.getSourceNames.head else sourceName
    config.getSourceConfig(name) match {
      case src: KafkaSourceConfig      => fromKafka(src)
      case src: KeyedKafkaSourceConfig => fromKeyedKafka(src)
      case src: KinesisSourceConfig    => fromKinesis(src)
      case src: FileSourceConfig       => fromFile(src)
      case src: SocketSourceConfig     => fromSocket(src)
      case src: CollectionSourceConfig => fromCollection(src)
      case src                         => throw new IllegalArgumentException(s"unsupported source connector: ${src.connector}")
    }
  }

  /**
    * Configure stream from kafka source.
    * @param srcConfig a source config
    * @param config implicitly provided job config
    * @tparam E stream element type
    * @return DataStream[E]
    */
  def fromKafka[E <: FlinkEvent: TypeInformation](
    srcConfig: KafkaSourceConfig
  )(implicit config: FlinkConfig,
    env: SEE
  ): DataStream[E] = {
    val consumer =
      new FlinkKafkaConsumer[E](srcConfig.topic,
                                config.getDeserializationSchema.asInstanceOf[DeserializationSchema[E]],
                                srcConfig.properties)
    env
      .addSource(consumer)
      .name(srcConfig.label)
  }

  /**
    * Configure stream from keyed kafka source.
    * @param srcConfig a source config
    * @param config implicitly provided job config
    * @tparam E stream element type
    * @return DataStream[E]
    */
  def fromKeyedKafka[E <: FlinkEvent: TypeInformation](
    srcConfig: KeyedKafkaSourceConfig
  )(implicit config: FlinkConfig,
    env: SEE
  ): DataStream[E] = {
    val consumer =
      new FlinkKafkaConsumer[E](srcConfig.topic,
                                config.getKeyedDeserializationSchema.asInstanceOf[KeyedDeserializationSchema[E]],
                                srcConfig.properties)
    env
      .addSource(consumer)
      .name(srcConfig.label)
  }

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
    * Configure stream from file source.
    * @param srcConfig a source config
    * @param config implicitly provided job config
    * @tparam E stream element type
    * @return DataStream[E]
    */
  def fromFile[E <: FlinkEvent: TypeInformation](
    srcConfig: FileSourceConfig
  )(implicit config: FlinkConfig,
    env: SEE
  ): DataStream[E] = {
    val path = srcConfig.path match {
      case RESOURCE_PATTERN(p) => getSourceFilePath(p)
      case other               => other
    }
    val ds = config.getDeserializationSchema.asInstanceOf[DeserializationSchema[E]]
    env
      .readTextFile(path)
      .map(line => ds.deserialize(line.getBytes(StandardCharsets.UTF_8)))
      .name(srcConfig.label)
  }

  /**
    * Configure stream from socket source.
    * @param srcConfig a source config
    * @param config implicitly provided job config
    * @tparam E stream element type
    * @return DataStream[E]
    */
  def fromSocket[E <: FlinkEvent: TypeInformation](
    srcConfig: SocketSourceConfig
  )(implicit config: FlinkConfig,
    env: SEE
  ): DataStream[E] =
    env
      .socketTextStream(srcConfig.host, srcConfig.port)
      .map(
        line =>
          config.getDeserializationSchema
            .asInstanceOf[DeserializationSchema[E]]
            .deserialize(line.getBytes(StandardCharsets.UTF_8))
      )
      .name(srcConfig.label)

  /**
    * Configure stream from collection source.
    * @param srcConfig a source config
    * @param config implicitly provided job config
    * @tparam E stream element type
    * @return DataStream[E]
    */
  def fromCollection[E <: FlinkEvent: TypeInformation](
    srcConfig: CollectionSourceConfig
  )(implicit config: FlinkConfig,
    env: SEE
  ): DataStream[E] =
    env
      .fromCollection[Array[Byte]](config.getCollectionSource(srcConfig.topic))
      .map(bytes => config.getDeserializationSchema.asInstanceOf[DeserializationSchema[E]].deserialize(bytes))
      .name(srcConfig.label)

  /**
    * Returns the actual path to a resource file named filename or filename.gz.
    *
    * @param filename the name of file
    * @return String
    */
  @throws[FileNotFoundException]
  def getSourceFilePath(filename: String): String = {
    val loader = getClass
    val resource = Option(loader.getResource(filename)) match {
      case Some(value) => value.toURI
      case None =>
        Option(loader.getResource(s"$filename.gz")) match {
          case Some(value) => value.toURI
          case None        => throw new FileNotFoundException(s"can't load resource $filename")
        }
    }
    val file = new File(resource)
    file.getAbsolutePath
  }

  implicit class EventStreamOps[E <: FlinkEvent: TypeInformation](stream: DataStream[E]) {

    def as[T <: FlinkEvent: TypeInformation]: DataStream[T] =
      stream.filter((e: E) => e.isInstanceOf[T]).map((e: E) => e.asInstanceOf[T])

    def toSink(sinkName: String = "")(implicit config: FlinkConfig) =
      StreamUtils.toSink[E](stream, sinkName)

  }

  /**
    * Configure stream sink from configuration.
    * @param stream the data stream to send to sink
    * @param sinkName a sink name to obtain configuration
    * @param config implicit flink job args
    * @tparam E stream element type
    * @return DataStream[E]
    */
  def toSink[E <: FlinkEvent: TypeInformation](
    stream: DataStream[E],
    sinkName: String = ""
  )(implicit config: FlinkConfig
  ): DataStreamSink[E] = {
    val name = if (sinkName.isEmpty) config.getSinkNames.head else sinkName
    config.getSinkConfig(name) match {
      case s: KafkaSinkConfig      => toKafka[E](stream, s)
      case s: KeyedKafkaSinkConfig => toKeyedKafka[E](stream, s)
      case s: KinesisSinkConfig    => toKinesis[E](stream, s)
      case s: FileSinkConfig       => toFile[E](stream, s)
      case s: SocketSinkConfig     => toSocket[E](stream, s)
      case s: JdbcSinkConfig       => toJdbc[E](stream, s)
      case s                       => throw new IllegalArgumentException(s"unsupported source connector: ${s.connector}")
    }
  }

  /**
    * Send stream to a kafka sink.
    * @param stream the data stream
    * @param sinkConfig a sink configuration
    * @param config implicit job args
    * @tparam E stream element type
    * @return DataStreamSink[E]
    */
  def toKafka[E <: FlinkEvent: TypeInformation](
    stream: DataStream[E],
    sinkConfig: KafkaSinkConfig
  )(implicit config: FlinkConfig
  ): DataStreamSink[E] =
    stream
      .addSink(
        new FlinkKafkaProducer[E](sinkConfig.topic,
                                  config.getSerializationSchema.asInstanceOf[SerializationSchema[E]],
                                  sinkConfig.properties)
      )
      .name(sinkConfig.label)

  /**
    * Send stream to a keyed kafka sink.
    * @param stream the data stream
    * @param sinkConfig a sink configuration
    * @param config implicit job args
    * @tparam E stream element type
    * @return DataStreamSink[E]
    */
  def toKeyedKafka[E <: FlinkEvent: TypeInformation](
    stream: DataStream[E],
    sinkConfig: KeyedKafkaSinkConfig
  )(implicit config: FlinkConfig
  ): DataStreamSink[E] =
    stream
      .addSink(
        new FlinkKafkaProducer[E](sinkConfig.topic,
                                  config.getKeyedSerializationSchema.asInstanceOf[KeyedSerializationSchema[E]],
                                  sinkConfig.properties)
      )
      .name(sinkConfig.label)

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
          new FlinkKinesisProducer[E](config.getSerializationSchema.asInstanceOf[KinesisSerializationSchema[E]],
                                      sinkConfig.properties)
        sink.setDefaultStream(sinkConfig.stream)
        sink.setFailOnError(true)
        sink
      })
      .name(sinkConfig.label)

  /**
    * Send stream to a socket sink.
    * @param stream the data stream
    * @param sinkConfig a sink configuration
    * @param config implicit job args
    * @tparam E stream element type
    * @return DataStreamSink[E]
    */
  def toJdbc[E <: FlinkEvent: TypeInformation](
    stream: DataStream[E],
    sinkConfig: JdbcSinkConfig
  )(implicit config: FlinkConfig
  ): DataStreamSink[E] =
    stream
      .addSink(
        new JdbcSink(config.getAddToJdbcBatchFunction.asInstanceOf[AddToJdbcBatchFunction[E]], sinkConfig.properties)
      )
      .name(sinkConfig.label)

  /**
    * Send stream to a rolling file sink.
    * @param stream the data stream
    * @param sinkConfig a sink configuration
    * @param config implicit job args
    * @tparam E stream element type
    * @return DataStreamSink[E]
    */
  def toFile[E <: FlinkEvent: TypeInformation](
    stream: DataStream[E],
    sinkConfig: FileSinkConfig
  )(implicit config: FlinkConfig
  ): DataStreamSink[E] = {

    val path = sinkConfig.path
    val p = sinkConfig.properties
    val bucketCheckInterval = p.getProperty("bucket.check.interval", s"${60000}").toLong
    val bucketAssigner = p.getProperty("bucket.assigner", "datetime") match {
      case "none" => new BasePathBucketAssigner[E]()
      case "datetime" =>
        val bucketFormat = p.getProperty("bucket.assigner.datetime.format", "yyyy/MM/dd/HH")
        new DateTimeBucketAssigner[E](bucketFormat)
      case assigner => throw new IllegalArgumentException(s"Unknown bucket assigner type: '$assigner'")
    }
    val encoderFormat = p.getProperty("encoder.format", "row")
    val sink = encoderFormat match {
      case "row" =>
        val builder = StreamingFileSink.forRowFormat(new Path(path), config.getEncoder.asInstanceOf[Encoder[E]])

        val rollingPolicy = p.getProperty("bucket.rolling.policy", "default") match {
          case "default" =>
            DefaultRollingPolicy
              .create()
              .withInactivityInterval(p.getProperty("bucket.rolling.policy.inactivity.interval", s"${60000}").toLong)
              .withMaxPartSize(p.getProperty("bucket.rolling.policy.max.part.size", s"${128 * 1024 * 1024}").toLong)
              .withRolloverInterval(
                p.getProperty("bucket.rolling.policy.rollover.interval", s"${Long.MaxValue}").toLong
              )
              .build[E, String]()
          case "checkpoint" => OnCheckpointRollingPolicy.build[E, String]()
          case policy       => throw new IllegalArgumentException(s"Unknown bucket rolling policy type: '$policy'")
        }
        builder
          .withBucketAssignerAndPolicy(bucketAssigner, rollingPolicy)
          .withBucketCheckInterval(bucketCheckInterval)
          .build()
      case "bulk" =>
        throw new NotImplementedError("Bulk file sink not implemented yet")

      case _ => throw new IllegalArgumentException(s"Unknown file sink encoder format: '$encoderFormat'")
    }
    stream.addSink(sink).name(sinkConfig.label)
  }

  /**
    * Send stream to a socket sink.
    * @param stream the data stream
    * @param sinkConfig a sink configuration
    * @param config implicit job args
    * @tparam E stream element type
    * @return DataStreamSink[E]
    */
  def toSocket[E <: FlinkEvent: TypeInformation](
    stream: DataStream[E],
    sinkConfig: SocketSinkConfig
  )(implicit config: FlinkConfig
  ): DataStreamSink[E] =
    stream.writeToSocket(sinkConfig.host,
                         sinkConfig.port,
                         config.getSerializationSchema.asInstanceOf[SerializationSchema[E]])

}
