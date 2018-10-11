package io.epiphanous.flinkrunner.util

import java.io.{File, FileNotFoundException}
import java.nio.charset.StandardCharsets
import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.flink.FlinkConnectorName._
import io.epiphanous.flinkrunner.flink._
import io.epiphanous.flinkrunner.model.FlinkEvent
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.contrib.streaming.state.{PredefinedOptions, RocksDBStateBackend}
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
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
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.streaming.connectors.kinesis.{FlinkKinesisConsumer, FlinkKinesisProducer}

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
    * Configure the streaming environment, including checkpointing, savepointing, and logging
    * @param args implicitly passed parameters
    * @return StreamExecutionEnvironment
    */
  def configureEnvironment(implicit args: Args): SEE = {

    if (args.missing.nonEmpty) {
      throw new NoSuchElementException(
        s"Missing job arguments:\n${args.missing.map(p => s"  - ${p.name}: ${p.text}").mkString("\n")}\n"
      )
    }

    val env =
      if (args.isDev) StreamExecutionEnvironment.createLocalEnvironment(1)
      else
        StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setGlobalJobParameters(args.params)

    // configure logging TODO
//    if (args.isProd) env.getConfig.disableSysoutLogging

    // use event time
    val timeCharacteristic = args.getString("time.characteristic") match {
      case "event" => TimeCharacteristic.EventTime
      case "ingestion" => TimeCharacteristic.IngestionTime
      case "processing" => TimeCharacteristic.ProcessingTime
      case unknown => throw new IllegalArgumentException(s"Unknown time characteristic: $unknown")
    }
    env.setStreamTimeCharacteristic(timeCharacteristic)

    // set parallelism
    env.setParallelism(args.getInt("global.parallelism"))

    // configure check-pointing and state backend
    val checkpointInterval = args.getLong("checkpoint.interval")
    if (checkpointInterval > 0) {
      env.enableCheckpointing(checkpointInterval)

      env.getCheckpointConfig.setMinPauseBetweenCheckpoints(args.getLong("checkpoint.min.pause"))

      env.getCheckpointConfig.setMaxConcurrentCheckpoints(args.getInt("checkpoint.max.concurrent"))

      val stateBackendUrl = args.getString("checkpoint.url")
      val backend = if (args.getString("state.backend") == "rocksdb") {
        logger.info(s"Using ROCKSDB state backend at $stateBackendUrl")
        val rocksBackend = new RocksDBStateBackend(
          stateBackendUrl,
          args.getBoolean("checkpoint.incremental")
        )
        if (args.getBoolean("checkpoint.flash"))
          rocksBackend.setPredefinedOptions(PredefinedOptions.FLASH_SSD_OPTIMIZED)
        rocksBackend
      } else {
        logger.info(s"Using FILE SYSTEM state backend at $stateBackendUrl")
        new FsStateBackend(stateBackendUrl)
      }
      env.setStateBackend(backend)
    }

    // return the environment
    env
  }

  /**
    * Generates a timestamp and watermark assigner for a stream with a given type of element that limits
    * how late an element is allowed to arrive in event time.
    *
    * @param lateness optional millisecond lateness (defaults to job param `max.allowed.lateness`)
    * @param args implicitly provided job args
    * @tparam E the type of stream element
    * @return BoundedLatenessGenerator[E]
    */
  def boundedLatenessEventTime[E <: FlinkEvent: TypeInformation](
      lateness: Option[Long] = None
    )(implicit args: Args
    ): BoundedLatenessGenerator[E] = {
    val allowedLateness = lateness.getOrElse(args.getLong("max.lateness"))
    new BoundedLatenessGenerator[E](allowedLateness)
  }

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
    * @param prefix a source prefix to obtain configuration
    * @param args implicit flink job args
    * @param env implicit flink streaming environment
    * @tparam E stream element type
    * @return DataStream[E]
    */
  def fromSource[E <: FlinkEvent: TypeInformation](
      prefix: String = "",
      sources: Map[String, Seq[Array[Byte]]]
    )(implicit args: Args,
      env: SEE
    ): DataStream[E] = {
    val config = args.getSourceConfig[E](prefix, sources)
    config.connector match {
      case FlinkConnectorName.Kinesis | FlinkConnectorName.Kafka => fromEventStore(config)
      case FlinkConnectorName.File => fromFile(config)
      case FlinkConnectorName.Socket => fromSocket(config)
      case FlinkConnectorName.Collection => fromCollection(config)
      case _ => throw new IllegalArgumentException(s"unsupported source connector: ${config.connector}")
    }
  }

  /**
    * Configure stream sink from configuration.
    * @param stream the data stream to send to sink
    * @param prefix a sink prefix to obtain configuration
    * @param args implicit flink job args
    * @tparam E stream element type
    * @return DataStream[E]
    */
  def toSink[E <: FlinkEvent: TypeInformation](stream: DataStream[E], prefix: String = "")(implicit args: Args) = {
    val config = args.getSinkConfig[E](prefix)
    config.connector match {
      case FlinkConnectorName.Kinesis | FlinkConnectorName.Kafka => toEventStore(stream, config)
      case FlinkConnectorName.File => toFile(stream, config)
      case FlinkConnectorName.Socket => toSocket(stream, config)
      case FlinkConnectorName.Jdbc => toJdbc(stream, config)
      case _ => throw new IllegalArgumentException(s"unsupported source connector: ${config.connector}")
    }
  }

  /**
    * Configure stream from event store source.
    * @param config a source config
    * @param args implicit flink job args
    * @param env implicit flink streaming environment
    * @tparam E stream element type
    * @return DataStream[E]
    */
  def fromEventStore[E <: FlinkEvent: TypeInformation](
      config: FlinkSourceConfig[E]
    )(implicit args: Args,
      env: SEE
    ): DataStream[E] = {
    val topic = config.getString("topic")
    val props = new Properties()
    props.putAll(config.props)
    props.remove("topic")
    val consumer = config.connector match {
      case Kinesis =>
        new FlinkKinesisConsumer[E](
          topic,
          config.deserializer,
          props
        )
      case Kafka =>
        new FlinkKafkaConsumer010[E](
          topic,
          config.deserializer,
          props
        )
      case _ => throw new IllegalArgumentException(s"Unsupported event store type: ${config.connector}")
    }
    env.addSource(consumer).name(s"${config.prefix}$topic")
  }

  /**
    * Configure stream from file source.
    * @param config a source config
    * @param args implicit flink job args
    * @param env implicit flink streaming environment
    * @tparam E stream element type
    * @return DataStream[E]
    */
  def fromFile[E <: FlinkEvent: TypeInformation](
      config: FlinkSourceConfig[E]
    )(implicit args: Args,
      env: SEE
    ): DataStream[E] = {
    val path = config.getString("path") match {
      case RESOURCE_PATTERN(p) => getSourceFilePath(p)
      case other => other
    }
    env.readTextFile(path).map(line => config.deserializer.deserialize(line.getBytes(StandardCharsets.UTF_8)))
  }

  /**
    * Configure stream from socket source.
    * @param config a source config
    * @param args implicit flink job args
    * @param env implicit flink streaming environment
    * @tparam E stream element type
    * @return DataStream[E]
    */
  def fromSocket[E <: FlinkEvent: TypeInformation](
      config: FlinkSourceConfig[E]
    )(implicit args: Args,
      env: SEE
    ): DataStream[E] = {
    val host = config.getString("host")
    val port = config.getInt("port")

    env
      .socketTextStream(host, port)
      .map(line => config.deserializer.deserialize(line.getBytes(StandardCharsets.UTF_8)))
      .name(s"${config.prefix}/socket://$host:$port")
  }

  /**
    * Configure stream from collection source.
    * @param config a source config
    * @param args implicit flink job args
    * @param env implicit flink streaming environment
    * @tparam E stream element type
    * @return DataStream[E]
    */
  def fromCollection[E <: FlinkEvent: TypeInformation](
      config: FlinkSourceConfig[E]
    )(implicit args: Args,
      env: SEE
    ): DataStream[E] = {
    val topic = config.getString("topic")
    val data = config.sources.getOrElse(topic, Seq.empty[Array[Byte]])
    env.fromCollection[Array[Byte]](data).map(bytes => config.deserializer.deserialize(bytes))
  }

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
          case None => throw new FileNotFoundException(s"can't load resource $filename")
        }
    }
    val file = new File(resource)
    file.getAbsolutePath
  }

  implicit class EventStreamOps[E <: FlinkEvent: TypeInformation](stream: DataStream[E]) {

    def as[T <: FlinkEvent: TypeInformation] =
      stream.filter(_.isInstanceOf[T]).map(_.asInstanceOf[T])

    def toSink(prefix: String)(implicit args: Args) =
      StreamUtils.toSink(stream, prefix)

  }

  /**
    * Write stream to event store sink.
    * @param stream of data elements
    * @param config a sink configuration
    * @param args the job args (implicit)
    * @tparam E the stream element type
    * @return DataStreamSink[E]
    */
  def toEventStore[E <: FlinkEvent: TypeInformation](
      stream: DataStream[E],
      config: FlinkSinkConfig[E]
    )(implicit
      args: Args
    ) = {
    val props = config.props
    val topic = config.getString("topic")
    val sink = config.connector match {
      case Kinesis =>
        val kp = config.serializer match {
          case Some(ss) => new FlinkKinesisProducer[E](ss, props)
          case None => throw new IllegalArgumentException(s"Missing or invalid serializer for ${config.prefix}")
        }
        kp.setDefaultStream(topic)
        kp.setFailOnError(true)
        kp
      case Kafka =>
        config.serializer match {
          case Some(ss) => new FlinkKafkaProducer010[E](topic, ss, props)
          case None =>
            config.keyedSerializer match {
              case Some(ks) => new FlinkKafkaProducer010[E](topic, ks, props)
              case None => throw new IllegalArgumentException(s"Missing or invalid serializer for ${config.prefix}")
            }
        }
      case _ => throw new IllegalArgumentException(s"Unknown event store type: '${config.connector}'")
    }
    stream.addSink(sink)
  }

  /**
    * Sends stream to jdbc sink.
    * @param stream the data stream
    * @param config a sink configuration
    * @param args implicit job args
    * @tparam E stream element type
    * @return DataStreamSink[E]
    */
  def toJdbc[E <: FlinkEvent: TypeInformation](
      stream: DataStream[E],
      config: FlinkSinkConfig[E]
    )(implicit args: FlinkJobArgs
    ): DataStreamSink[E] =
    stream.addSink(
      new JdbcSink(config.jdbcBatchFunction.getOrElse(
                     throw new IllegalArgumentException(s"Missing '${config.prefix}batch.function' property.")),
                   config.props))

  /**
    * Send stream to a file sink.
    * @param stream the data stream
    * @param config a sink configuration
    * @param args implicit job args
    * @tparam E stream element type
    * @return DataStreamSink[E]
    */
  def toFile[E <: FlinkEvent: TypeInformation](
      stream: DataStream[E],
      config: FlinkSinkConfig[E]
    )(implicit args: FlinkJobArgs
    ): DataStreamSink[E] = {

    val path = config.getString("path")
    val bucketCheckInterval = config.getLong("bucket.check.interval")
    val bucketAssigner = config.getString("bucket.assigner") match {
      case "none" => new BasePathBucketAssigner[E]()
      case "datetime" =>
        val bucketFormat = config.getString("bucket.assigner.datetime.format")
        new DateTimeBucketAssigner[E](bucketFormat)
      case assigner => throw new IllegalArgumentException(s"Unknown bucket assigner type: '$assigner'")
    }
    val encoderFormat = config.getString("encoder.format")
    val sink = encoderFormat match {
      case "row" =>
        val encoder =
          config.fileEncoder.getOrElse(
            throw new IllegalArgumentException(s"Missing '${config.prefix}encoder.class' property for file sink"))
        val builder = StreamingFileSink.forRowFormat(new Path(path), encoder)

        val rollingPolicy = config.getString("bucket.rolling.policy") match {
          case "default" =>
            DefaultRollingPolicy
              .create()
              .withInactivityInterval(config.getLong("bucket.rolling.policy.inactivity.interval"))
              .withMaxPartSize(config.getLong("bucket.rolling.policy.max.part.size"))
              .withRolloverInterval(config.getLong("bucket.rolling.policy.rollover.interval"))
              .build[E, String]()
          case "checkpoint" => OnCheckpointRollingPolicy.build[E, String]()
          case policy => throw new IllegalArgumentException(s"Unknown bucket rolling policy type: '$policy'")
        }
        builder
          .withBucketAssignerAndPolicy(bucketAssigner, rollingPolicy)
          .withBucketCheckInterval(bucketCheckInterval)
          .build()
      case "bulk" =>
        throw new NotImplementedError("Bulk file sink not implemented yet")

      case _ => throw new IllegalArgumentException(s"Unknown file sink encoder format: '$encoderFormat'")
    }
    stream.addSink(sink)
  }

  /**
    * Send stream to a socket sink.
    * @param stream the data stream
    * @param config a sink configuration
    * @param args implicit job args
    * @tparam E stream element type
    * @return DataStreamSink[E]
    */
  def toSocket[E <: FlinkEvent: TypeInformation](
      stream: DataStream[E],
      config: FlinkSinkConfig[E]
    )(implicit args: FlinkJobArgs
    ): DataStreamSink[E] = {
    val host = config.getString("host")
    val port = config.getInt("port")
    val serializer = config.serializer.getOrElse(
      throw new IllegalArgumentException(s"Missing '${config.prefix}serializer.class' property in socket sink"))
    stream.writeToSocket(host, port, serializer)
  }

}
