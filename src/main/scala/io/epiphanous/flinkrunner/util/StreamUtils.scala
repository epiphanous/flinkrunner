package io.epiphanous.flinkrunner.util

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.SEE
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.operator.AddToJdbcBatchFunction
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.{
  DeserializationSchema,
  Encoder,
  SerializationSchema
}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.{
  BasePathBucketAssigner,
  DateTimeBucketAssigner
}
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.{
  DefaultRollingPolicy,
  OnCheckpointRollingPolicy
}
import org.apache.flink.streaming.api.functions.sink.filesystem.{
  BucketAssigner,
  StreamingFileSink
}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.cassandra.CassandraSink
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.flink.streaming.connectors.kafka.{
  FlinkKafkaConsumer,
  FlinkKafkaProducer,
  KafkaDeserializationSchema,
  KafkaSerializationSchema
}
import org.apache.flink.streaming.connectors.kinesis.serialization.{
  KinesisDeserializationSchema,
  KinesisSerializationSchema
}
import org.apache.flink.streaming.connectors.kinesis.{
  FlinkKinesisConsumer,
  FlinkKinesisProducer
}
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import java.io.{File, FileNotFoundException}
import java.net.URL
import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._
import scala.util.matching.Regex

object StreamUtils extends LazyLogging {

  val RESOURCE_PATTERN: Regex = "resource://(.*)".r

  /**
   * A little syntactic sugar for writing stream program. This is the pipe
   * operator, ala F#.
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
   * @param v
   *   any object
   * @tparam A
   *   the type of v
   */
  implicit class Pipe[A](val v: A) extends AnyVal {
    // forward pipe op
    def |>[B](t: A => B): B = t(v)

    // side effect op
    def |#(e: A => Unit): A = {
      e(v); v
    }
  }

  /**
   * Generates a timestamp and watermark assigner for a stream with a given
   * type of element that limits how late an element is allowed to arrive
   * in event time.
   *
   * @param config
   *   implicitly provided job config
   * @tparam E
   *   the type of stream element
   * @return
   *   BoundedLatenessGenerator[E]
   */
  def boundedLatenessWatermarks[E <: FlinkEvent: TypeInformation](
      streamID: String
  )(implicit config: FlinkConfig) =
    new BoundedLatenessWatermarkStrategy[E](
      config.maxLateness.toMillis,
      streamID
    )

  /**
   * Create a bounded of order watermark strategy with idleness checking
   *
   * @param config
   *   implicitly provided job config
   * @tparam E
   *   the type of stream element
   * @return
   *   BoundedLatenessGenerator[E]
   */
  def boundedOutofOrdernessWatermarks[E <: FlinkEvent: TypeInformation]()(
      implicit config: FlinkConfig): WatermarkStrategy[E] =
    WatermarkStrategy
      .forBoundedOutOfOrderness(config.maxLateness)
      .withIdleness(config.maxIdleness)

  /**
   * Creates an ascending timestamp watermark strategy.
   * @tparam E
   *   type of stream element
   * @return
   *   AscendingTimestampExtractor[E]
   */
  def ascendingTimestampsWatermarks[E <: FlinkEvent: TypeInformation]()
      : WatermarkStrategy[E] = WatermarkStrategy.forMonotonousTimestamps()

  /**
   * Assign timestamps/watermarks if we're using event time
   * @param in
   *   the input stream to watermark
   * @param config
   *   implicit flink configuration
   * @param env
   *   implicit stream execution environment
   * @tparam E
   *   event type
   * @return
   *   the possibly watermarked input stream
   */
  def maybeAssignTimestampsAndWatermarks[E <: FlinkEvent: TypeInformation](
      in: DataStream[E],
      srcConfig: SourceConfig
  )(implicit config: FlinkConfig): DataStream[E] =
    in.assignTimestampsAndWatermarks(srcConfig.watermarkStrategy match {
      case "bounded out of orderness" =>
        boundedOutofOrdernessWatermarks()
      case "ascending timestamps"     => ascendingTimestampsWatermarks()
      case _                          => boundedLatenessWatermarks(in.name)
    }).name(s"wm:${in.name}")
      .uid(s"wm:${in.name}")

  /**
   * Configure stream source from configuration.
   *
   * @param sourceName
   *   the name of the source to get its configuration
   * @tparam E
   *   stream element type
   * @return
   *   DataStream[E]
   */
  def fromSource[E <: FlinkEvent: TypeInformation](
      sourceName: String = ""
  )(implicit config: FlinkConfig, env: SEE): DataStream[E] = {
    val name   =
      if (sourceName.isEmpty) config.getSourceNames.head else sourceName
    val src    = config.getSourceConfig(name)
    val uid    = src.label
    val stream = (src match {
      case src: KafkaSourceConfig      => fromKafka(src)
      case src: KinesisSourceConfig    => fromKinesis(src)
      case src: FileSourceConfig       => fromFile(src)
      case src: SocketSourceConfig     => fromSocket(src)
      case src: CollectionSourceConfig => fromCollection(src)
      case src                         =>
        throw new IllegalArgumentException(
          s"unsupported source connector: ${src.connector}"
        )
    }).name(uid).uid(uid)
    maybeAssignTimestampsAndWatermarks(stream, src)
  }

  /**
   * Configure stream from kafka source.
   *
   * @param srcConfig
   *   a source config
   * @param config
   *   implicitly provided job config
   * @tparam E
   *   stream element type
   * @return
   *   DataStream[E]
   */
  def fromKafka[E <: FlinkEvent: TypeInformation](
      srcConfig: KafkaSourceConfig
  )(implicit config: FlinkConfig, env: SEE): DataStream[E] = {
    val consumer =
      new FlinkKafkaConsumer[E](
        srcConfig.topic,
        config
          .getKafkaDeserializationSchema(srcConfig.name)
          .asInstanceOf[KafkaDeserializationSchema[E]],
        srcConfig.properties
      )
    env
      .addSource(consumer)
  }

  /**
   * Configure stream from kinesis.
   *
   * @param srcConfig
   *   a source config
   * @param config
   *   implicitly provided job config
   * @tparam E
   *   stream element type
   * @return
   *   DataStream[E]
   */
  def fromKinesis[E <: FlinkEvent: TypeInformation](
      srcConfig: KinesisSourceConfig
  )(implicit config: FlinkConfig, env: SEE): DataStream[E] = {
    val consumer =
      new FlinkKinesisConsumer[E](
        srcConfig.stream,
        config
          .getKinesisDeserializationSchema(srcConfig.name)
          .asInstanceOf[KinesisDeserializationSchema[E]],
        srcConfig.properties
      )
    env
      .addSource(consumer)
      .name(srcConfig.label)
  }

  /**
   * Configure stream from file source.
   *
   * @param srcConfig
   *   a source config
   * @param config
   *   implicitly provided job config
   * @tparam E
   *   stream element type
   * @return
   *   DataStream[E]
   */
  def fromFile[E <: FlinkEvent: TypeInformation](
      srcConfig: FileSourceConfig
  )(implicit config: FlinkConfig, env: SEE): DataStream[E] = {
    val path = srcConfig.path match {
      case RESOURCE_PATTERN(p) => getSourceFilePath(p)
      case other               => other
    }
    val ds   = config
      .getDeserializationSchema(srcConfig.name)
      .asInstanceOf[DeserializationSchema[E]]
    env
      .readTextFile(path)
      .name(s"raw:${srcConfig.label}")
      .uid(s"raw:${srcConfig.label}")
      .map(line => ds.deserialize(line.getBytes(StandardCharsets.UTF_8)))
  }

  /**
   * Configure stream from socket source.
   *
   * @param srcConfig
   *   a source config
   * @param config
   *   implicitly provided job config
   * @tparam E
   *   stream element type
   * @return
   *   DataStream[E]
   */
  def fromSocket[E <: FlinkEvent: TypeInformation](
      srcConfig: SocketSourceConfig
  )(implicit config: FlinkConfig, env: SEE): DataStream[E] =
    env
      .socketTextStream(srcConfig.host, srcConfig.port)
      .name(s"raw:${srcConfig.label}")
      .uid(s"raw:${srcConfig.label}")
      .map(line =>
        config
          .getDeserializationSchema(srcConfig.name)
          .asInstanceOf[DeserializationSchema[E]]
          .deserialize(line.getBytes(StandardCharsets.UTF_8))
      )

  /**
   * Configure stream from collection source.
   *
   * @param srcConfig
   *   a source config
   * @param config
   *   implicitly provided job config
   * @tparam E
   *   stream element type
   * @return
   *   DataStream[E]
   */
  def fromCollection[E <: FlinkEvent: TypeInformation](
      srcConfig: CollectionSourceConfig
  )(implicit config: FlinkConfig, env: SEE): DataStream[E] =
    env
      .fromCollection[Array[Byte]](
        config.getCollectionSource(srcConfig.topic)
      )
      .name(s"raw:${srcConfig.label}")
      .uid(s"raw:${srcConfig.label}")
      .map(bytes =>
        config
          .getDeserializationSchema(srcConfig.name)
          .asInstanceOf[DeserializationSchema[E]]
          .deserialize(bytes)
      )

  /**
   * Returns the actual path to a resource file named filename or
   * filename.gz.
   *
   * @param filename
   *   the name of file
   * @return
   *   String
   */
  @throws[FileNotFoundException]
  def getSourceFilePath(filename: String): String = {
    val loader   = getClass
    val resource = Option(loader.getResource(filename)) match {
      case Some(value) => value.toURI
      case None        =>
        Option(loader.getResource(s"$filename.gz")) match {
          case Some(value) => value.toURI
          case None        =>
            throw new FileNotFoundException(
              s"can't load resource $filename"
            )
        }
    }
    val file     = new File(resource)
    file.getAbsolutePath
  }

  implicit class EventStreamOps[E <: FlinkEvent: TypeInformation](
      stream: DataStream[E]) {

    def as[T <: FlinkEvent: TypeInformation]: DataStream[T] = {
      val name = stream.name
      stream
        .filter((e: E) => e.isInstanceOf[T @unchecked])
        .name(s"filter types $name")
        .uid(s"filter types $name")
        .map((e: E) => e.asInstanceOf[T @unchecked])
        .name(s"cast types $name")
        .uid(s"cast types $name")
    }

    def toSink(sinkName: String = "")(implicit config: FlinkConfig) =
      StreamUtils.toSink[E](stream, sinkName)

  }

  /**
   * Configure stream sink from configuration.
   *
   * @param stream
   *   the data stream to send to sink
   * @param sinkName
   *   a sink name to obtain configuration
   * @param config
   *   implicit flink job args
   * @tparam E
   *   stream element type
   * @return
   *   DataStream[E]
   */
  def toSink[E <: FlinkEvent: TypeInformation](
      stream: DataStream[E],
      sinkName: String = ""
  )(implicit config: FlinkConfig) = {
    val name = if (sinkName.isEmpty) config.getSinkNames.head else sinkName
    config.getSinkConfig(name) match {
      case s: KafkaSinkConfig         => toKafka[E](stream, s)
      case s: KinesisSinkConfig       => toKinesis[E](stream, s)
      case s: FileSinkConfig          => toFile[E](stream, s)
      case s: SocketSinkConfig        => toSocket[E](stream, s)
      case s: JdbcSinkConfig          => toJdbc[E](stream, s)
      case s: CassandraSinkConfig     => toCassandraSink[E](stream, s)
      case s: ElasticsearchSinkConfig => toElasticsearchSink[E](stream, s)
      case s                          =>
        throw new IllegalArgumentException(
          s"unsupported source connector: ${s.connector}"
        )
    }
  }

  /**
   * Send stream to a kafka sink.
   *
   * @param stream
   *   the data stream
   * @param sinkConfig
   *   a sink configuration
   * @param config
   *   implicit job args
   * @tparam E
   *   stream element type
   * @return
   *   DataStreamSink[E]
   */
  def toKafka[E <: FlinkEvent: TypeInformation](
      stream: DataStream[E],
      sinkConfig: KafkaSinkConfig
  )(implicit config: FlinkConfig): DataStreamSink[E] =
    stream
      .addSink(
        new FlinkKafkaProducer[E](
          sinkConfig.topic,
          config
            .getKafkaSerializationSchema(sinkConfig.name)
            .asInstanceOf[KafkaSerializationSchema[E]],
          sinkConfig.properties,
          Semantic.AT_LEAST_ONCE
        )
      )
      .uid(sinkConfig.label)
      .name(sinkConfig.label)

  /**
   * Send stream to a kinesis sink.
   *
   * @param stream
   *   the data stream
   * @param sinkConfig
   *   a sink configuration
   * @param config
   *   implicit job args
   * @tparam E
   *   stream element type
   * @return
   *   DataStreamSink[E]
   */
  def toKinesis[E <: FlinkEvent: TypeInformation](
      stream: DataStream[E],
      sinkConfig: KinesisSinkConfig
  )(implicit config: FlinkConfig): DataStreamSink[E] =
    stream
      .addSink {
        val sink =
          new FlinkKinesisProducer[E](
            config
              .getKinesisSerializationSchema(sinkConfig.name)
              .asInstanceOf[KinesisSerializationSchema[E]],
            sinkConfig.properties
          )
        sink.setDefaultStream(sinkConfig.stream)
        sink.setFailOnError(true)
        sink.setDefaultPartition("0")
        sink
      }
      .uid(sinkConfig.label)
      .name(sinkConfig.label)

  /**
   * Send stream to a socket sink.
   *
   * @param stream
   *   the data stream
   * @param sinkConfig
   *   a sink configuration
   * @param config
   *   implicit job args
   * @tparam E
   *   stream element type
   * @return
   *   DataStreamSink[E]
   */
  def toJdbc[E <: FlinkEvent: TypeInformation](
      stream: DataStream[E],
      sinkConfig: JdbcSinkConfig
  )(implicit config: FlinkConfig): DataStreamSink[E] =
    stream
      .addSink(
        new JdbcSink(
          config
            .getAddToJdbcBatchFunction(sinkConfig.name)
            .asInstanceOf[AddToJdbcBatchFunction[E]],
          sinkConfig.properties
        )
      )
      .uid(sinkConfig.label)
      .name(sinkConfig.label)

  /**
   * Send stream to a rolling file sink.
   *
   * @param stream
   *   the data stream
   * @param sinkConfig
   *   a sink configuration
   * @param config
   *   implicit job args
   * @tparam E
   *   stream element type
   * @return
   *   DataStreamSink[E]
   */
  def toFile[E <: FlinkEvent: TypeInformation](
      stream: DataStream[E],
      sinkConfig: FileSinkConfig
  )(implicit config: FlinkConfig): DataStreamSink[E] = {
    val path                = sinkConfig.path
    val p                   = sinkConfig.properties
    val bucketCheckInterval =
      p.getProperty("bucket.check.interval", s"${60000}").toLong
    val bucketAssigner      =
      p.getProperty("bucket.assigner.type", "datetime") match {
        case "none"     => new BasePathBucketAssigner[E]()
        case "datetime" =>
          new DateTimeBucketAssigner[E](
            p.getProperty(
              "bucket.assigner.datetime.format",
              "YYYY/MM/DD/HH"
            )
          )
        case "custom"   =>
          config
            .getBucketAssigner(sinkConfig.name)
            .asInstanceOf[BucketAssigner[E, String]]
        case other      =>
          throw new IllegalArgumentException(
            s"Unknown bucket assigner type '$other'."
          )
      }
    val encoderFormat       = p.getProperty("encoder.format", "row")
    val sink                = encoderFormat match {
      case "row"  =>
        val builder       =
          StreamingFileSink.forRowFormat(
            new Path(path),
            config.getEncoder(sinkConfig.name).asInstanceOf[Encoder[E]]
          )
        val rollingPolicy =
          p.getProperty("bucket.rolling.policy", "default") match {
            case "default"    =>
              DefaultRollingPolicy
                .builder()
                .withInactivityInterval(
                  p.getProperty(
                    "bucket.rolling.policy.inactivity.interval",
                    s"${60000}"
                  ).toLong
                )
                .withMaxPartSize(
                  p.getProperty(
                    "bucket.rolling.policy.max.part.size",
                    s"${128 * 1024 * 1024}"
                  ).toLong
                )
                .withRolloverInterval(
                  p.getProperty(
                    "bucket.rolling.policy.rollover.interval",
                    s"${Long.MaxValue}"
                  ).toLong
                )
                .build[E, String]()
            case "checkpoint" =>
              OnCheckpointRollingPolicy.build[E, String]()
            case policy       =>
              throw new IllegalArgumentException(
                s"Unknown bucket rolling policy type: '$policy'"
              )
          }
        builder
          .withBucketAssigner(bucketAssigner)
          .withRollingPolicy(rollingPolicy)
          .withBucketCheckInterval(bucketCheckInterval)
          .build()
      case "bulk" =>
        throw new NotImplementedError("Bulk file sink not implemented yet")

      case _ =>
        throw new IllegalArgumentException(
          s"Unknown file sink encoder format: '$encoderFormat'"
        )
    }
    stream.addSink(sink).uid(sinkConfig.label).name(sinkConfig.label)
  }

  /**
   * Send stream to a socket sink.
   *
   * @param stream
   *   the data stream
   * @param sinkConfig
   *   a sink configuration
   * @param config
   *   implicit job args
   * @tparam E
   *   stream element type
   * @return
   *   DataStreamSink[E]
   */
  def toSocket[E <: FlinkEvent: TypeInformation](
      stream: DataStream[E],
      sinkConfig: SocketSinkConfig
  )(implicit config: FlinkConfig): DataStreamSink[E] =
    stream
      .writeToSocket(
        sinkConfig.host,
        sinkConfig.port,
        config
          .getSerializationSchema(sinkConfig.name)
          .asInstanceOf[SerializationSchema[E]]
      )
      .uid(sinkConfig.label)
      .name(sinkConfig.label)

  /**
   * Send stream to a cassandra sink.
   *
   * @param stream
   *   the data stream
   * @param sinkConfig
   *   a sink configuration
   * @tparam E
   *   stream element type
   * @return
   *   DataStreamSink[E]
   */
  def toCassandraSink[E <: FlinkEvent: TypeInformation](
      stream: DataStream[E],
      sinkConfig: CassandraSinkConfig) =
    CassandraSink
      .addSink(stream)
      .setHost(sinkConfig.host)
      .setQuery(sinkConfig.query)
      .build()
      .uid(sinkConfig.label)
      .name(sinkConfig.label)

  /**
   * Send stream to an elasticsearch sink.
   *
   * @param stream
   *   the data stream
   * @param sinkConfig
   *   a sink configuration
   * @tparam E
   *   stream element type
   * @return
   *   DataStreamSink[E]
   */
  def toElasticsearchSink[E <: FlinkEvent: TypeInformation](
      stream: DataStream[E],
      sinkConfig: ElasticsearchSinkConfig
  ): DataStreamSink[E] = {
    val hosts  = sinkConfig.transports.map { s =>
      val url      = new URL(if (s.startsWith("http")) s else s"http://$s")
      val hostname = url.getHost
      val port     = if (url.getPort < 0) 9200 else url.getPort
      new HttpHost(hostname, port, url.getProtocol)
    }.asJava
    val esSink = new ElasticsearchSink.Builder[E](
      hosts,
      (element: E, _: RuntimeContext, indexer: RequestIndexer) => {
        val data = element.getClass.getDeclaredFields
          .filterNot(f =>
            Seq("$id", "$key", "$timestamp", "$action").contains(
              f.getName
            )
          )
          .foldLeft(Map.empty[String, Any]) { case (a, f) =>
            f.setAccessible(true)
            val name = f.getName
            f.get(element) match {
              case Some(v: Any) => a + (name -> v)
              case None         => a
              case v: Any       => a + (name -> v)
            }
          }
          .asJava
        val req  = Requests.indexRequest(sinkConfig.index).source(data)
        indexer.add(req)
      }
    ).build()
    stream.addSink(esSink).uid(sinkConfig.label).name(sinkConfig.label)
  }

}
