package io.epiphanous.flinkrunner.util

import java.io.{File, FileNotFoundException}
import java.nio.charset.StandardCharsets
import java.sql.PreparedStatement

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.flink.{Args, FlinkJobArgs, SEE}
import io.epiphanous.flinkrunner.model.FlinkEvent
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.contrib.streaming.state.{PredefinedOptions, RocksDBStateBackend}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor.IgnoringHandler
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema

object StreamUtils extends LazyLogging {

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
      if (args.mockEdges) StreamExecutionEnvironment.createLocalEnvironment(1)
      else
        StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setGlobalJobParameters(args.params)

    // configure logging TODO
//    if (args.isProd) env.getConfig.disableSysoutLogging

    // use event time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // set parallelism
    env.setParallelism(args.getInt("global.parallelism"))

    // configure check-pointing and state backend
    env.enableCheckpointing(args.getLong("checkpoint.interval"))

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

    // return the environment
    env
  }

  /**
    * Generates a timestamp and watermark assigner for a stream with a given type of element that limits
    * how late an element is allowed to arrive in event time.
    *
    * @param getTimestamp function to get the timestamp from a stream element
    * @param lateness optional millisecond lateness (defaults to job param `max.allowed.lateness`)
    * @param args implicitly provided job args
    * @tparam E the type of stream element
    * @return BoundedLatenessGenerator[E]
    */
  def boundedLatenessEventTime[E <: FlinkEvent: TypeInformation](
//      getTimestamp: E => Long,
      lateness: Option[Long] = None
    )(implicit args: Args
    ): BoundedLatenessGenerator[E] = {
    val allowedLateness = lateness.getOrElse(args.getLong("max.lateness"))
    new BoundedLatenessGenerator[E]( /*getTimestamp: E => Long, */ allowedLateness)
  }

  /**
    * Creates an ascending timestamp extractor.
    * @param getTimestamp function to get the timestamp from a stream element
    * @tparam E type of stream element
    * @return AscendingTimestampExtractor[ FlinkEvent[S] ]
    */
  def ascendingTimestampExtractor[E <: FlinkEvent: TypeInformation](
      getTimestamp: E => Long
    ): AscendingTimestampExtractor[E] = {
    val extractor: AscendingTimestampExtractor[E] = new AscendingTimestampExtractor[E] {
      var lastTimestamp = Long.MinValue
      def extractAscendingTimestamp(event: E) = {
        lastTimestamp = getTimestamp(event)
        lastTimestamp
      }
    }
    extractor.withViolationHandler(new IgnoringHandler())
    extractor
  }

  /**
    * Obtain a stream of elements from kafka. Gets kafka configuration info from the implicit streaming environment.
    *
    * If mockEdges is true, attempts to load the stream from the
    * sources map or the resource path, using the configured topic name as the key in the map or the file
    * name.
    *
    * @param sources a map of topics to sequence of raw json strings [for testing]
    * @param deserializationSchema a flink deserialization schema to instantiate stream elements from kafka, the map or the file system
    * @param prefix a string to prefix the kafka.source configuration
    * @param args implicitly passed parameters (for kafka configuration)
    * @param env implicitly passed streaming environment
    * @tparam E the type of stream element
    * @return DataStream[E]
    */
  def fromKafka[E <: FlinkEvent: TypeInformation](
      sources: Map[String, Seq[Array[Byte]]],
      deserializationSchema: DeserializationSchema[E],
      prefix: String = ""
    )(implicit args: Args,
      env: SEE
    ): DataStream[E] = {

    val props = args.getProps(List(prefix, "kafka.source"))
    val topic = props.getProperty("topic")
    props.remove("topic")

    val stream = if (args.mockEdges) {
      sources.get(topic) match {
        // try loading from sources map
        case Some(seq) =>
          env.fromCollection(seq.map(b => deserializationSchema.deserialize(b)))

        // try loading from file
        case None => fromCollection[E](topic, deserializationSchema)
      }
    } else
      env.addSource(new FlinkKafkaConsumer010(
        topic,
        deserializationSchema,
        props
      ))
    stream.name(s"$prefix/$topic")
  }

  /**
    * Instantiate event stream from a json file on the classpath under "in/${topic}.json[.gz]".
    *
    * Used for mocking kafka json sources.
    *
    * @param topic the root name of the json file (same as mocked kafka topic)
    * @param deserializationSchema to deserialize json to stream objects of type E
    * @param args implicitly provided flink job args
    * @param env implicitly provided stream execution environment
    * @tparam E data stream element type
    * @return DataStream[E]
    */
  def fromCollection[E <: FlinkEvent: TypeInformation](
      topic: String,
      deserializationSchema: DeserializationSchema[E]
    )(implicit args: Args,
      env: SEE
    ) = {
    env
      .readTextFile(getSourceFilePath(topic))
      .map(json => deserializationSchema.deserialize(json.getBytes(StandardCharsets.UTF_8)))
  }

  /**
    * Returns the path to a json resource file named "in/${topic}.json[.gz]". Supports mocking kafka.
    *
    * @param topic the root name of the json file
    * @return String
    */
  @throws[FileNotFoundException]
  def getSourceFilePath(topic: String): String = {
    val filename = s"/in/$topic.json"
    val loader   = getClass
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

    def as[T <: FlinkEvent: TypeInformation] =
      stream.filter(_.isInstanceOf[T]).map(_.asInstanceOf[T])

    def toKafka(serializationSchema: SerializationSchema[E], prefix: String = "")(implicit args: Args) =
      StreamUtils.toKafka[E](stream, serializationSchema, prefix)

    def toKafkaKeyed(serializationSchema: KeyedSerializationSchema[E], prefix: String = "")(implicit args: Args) =
      StreamUtils.toKafkaKeyed[E](stream, serializationSchema, prefix)

    def toJdbc(query: String, addToBatch: (E, PreparedStatement) => Unit, prefix: String = "")(implicit args: Args) =
      StreamUtils.toJdbc[E](stream, query, addToBatch, prefix)
  }

  /**
    * Write a stream of elements to Kafka. Uses any kafka properties it finds in the implicitly
    * provided `args` that start with `kafka.producer.`.
    *
    * @param stream of data elements
    * @param args the job args (implicit)
    * @tparam E the stream element type
    * @return DataStreamSink[E]
    */
  def toKafka[E <: FlinkEvent: TypeInformation](
      stream: DataStream[E],
      serializationSchema: SerializationSchema[E],
      prefix: String = ""
    )(implicit
      args: Args
    ) = {
    val props = args.getProps(List(prefix, "kafka.sink"))
    val topic = props.getProperty("topic")
    props.remove("topic")
    stream.addSink(new FlinkKafkaProducer010[E](topic, serializationSchema, props))
  }

  /**
    * Write a keyed stream of elements to Kafka. Uses any kafka properties it finds in the implicitly
    * provided `args` that start with `${prefix}kafka.sink.`.
    *
    * @param args the job args (implicit)
    * @tparam E the stream element type
    * @return DataStreamSink[E]
    */
  def toKafkaKeyed[E <: FlinkEvent: TypeInformation](
      stream: DataStream[E],
      serializationSchema: KeyedSerializationSchema[E],
      prefix: String = ""
    )(implicit args: Args
    ) = {
    val props = args.getProps(List("", "kafka.sink"))
    val topic = props.getProperty("topic")
    props.remove("topic")
    stream.addSink(new FlinkKafkaProducer010[E](topic, serializationSchema, props))
  }

  /**
    * Sends stream elements to jdbc, using the provided query and configuration. The `addToBatch` function
    * adds each row to a prepared statement.
    *
    * The configuration comes from `${prefix}.jdbc.sink` implicit job args.
    *
    * @param stream the data stream
    * @param query the query to prepare
    * @param addToBatch a function that adds an element's data to a prepared statement
    * @param prefix a prefix for the jdbc configuration parameters
    * @param args implicit job args
    * @tparam E stream element type
    * @return DataStreamSink[E]
    */
  def toJdbc[E <: FlinkEvent: TypeInformation](
      stream: DataStream[E],
      query: String,
      addToBatch: (E, PreparedStatement) => Unit,
      prefix: String = ""
    )(implicit args: FlinkJobArgs
    ): DataStreamSink[E] = {
    val props = args.getProps(List(prefix, "jdbc.sink"))
    props.setProperty("query", query)
    stream.addSink(new JdbcSink[E](addToBatch, props))
  }

}
