package io.epiphanous.flinkrunner

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.util.BoundedLatenessWatermarkStrategy
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.{
  DeserializationSchema,
  Encoder,
  SerializationSchema
}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.jdbc.{
  JdbcConnectionOptions,
  JdbcExecutionOptions,
  JdbcSink,
  JdbcStatementBuilder
}
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.source.KafkaSource
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
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.connectors.cassandra.CassandraSink
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase.FlushBackoffType
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisSerializationSchema
import org.apache.flink.streaming.connectors.kinesis.{
  FlinkKinesisConsumer,
  FlinkKinesisProducer
}
import org.apache.flink.streaming.connectors.rabbitmq.{RMQSink, RMQSource}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import java.io.{File, FileNotFoundException}
import java.net.URL
import java.nio.charset.StandardCharsets
import java.sql.PreparedStatement
import scala.collection.JavaConverters._
import scala.util.matching.Regex

/**
 * Flink Job Invoker
 */
class FlinkRunner[ADT <: FlinkEvent](
    args: Array[String],
    factory: FlinkRunnerFactory[ADT],
    sources: Map[String, Seq[Array[Byte]]] = Map.empty,
    optConfig: Option[String] = None)
    extends LazyLogging {

  val config: FlinkConfig[ADT]         =
    factory.getFlinkConfig(args, sources, optConfig)
  val env: StreamExecutionEnvironment  =
    config.configureStreamExecutionEnvironment
  val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

  /**
   * Invoke a job based on the job name and arguments passed in. If the job
   * run returns an iterator of results, pass those results to the
   * callback. Otherwise, just return. The callback is for testing the
   * stream of results from a flink job. It will only be invoked if
   * --mock.edges option is on.
   *
   * @param callback
   *   a function from a stream to unit that receives results from running
   *   flink job
   */
  def process(
      callback: PartialFunction[List[_], Unit] = { case _ =>
        ()
      }
  ): Unit = {
    if (config.jobName == "help") showHelp()
    else if (
      config.jobArgs.headOption
        .exists(s => List("help", "--help", "-help", "-h").contains(s))
    ) showJobHelp()
    else {
      factory.getJobInstance(config.jobName, this).run() match {
        case Left(results) => callback(results)
        case Right(_)      => ()
      }
    }
  }

  /**
   * Show help for a particular job
   */
  def showJobHelp(): Unit = {
    val usage =
      s"""|${config.jobName} - ${config.jobDescription}
          |
          |Usage: ${config.systemName} ${config.jobName} [job parameters]
          |${config.jobHelp}
       """.stripMargin
    println(usage)
  }

  /**
   * Show a help message regarding usage.
   *
   * @param error
   *   an optional error message to show
   */
  def showHelp(error: Option[String] = None): Unit = {
    val jobInfo = config.jobs.toList.sorted match {
      case s if s.isEmpty => "  *** No jobs defined ***"
      case s              =>
        s.map { jn =>
          val desc = config.getString(s"jobs.$jn.description")
          s"  - $jn: $desc"
        }.mkString("\n")
    }
    val usage   =
      s"""|
          |Usage: ${config.systemName} <jobName> [job parameters]
          |
          |Jobs:
          |
          |$jobInfo
          |
          |Try "${config.systemName} <jobName> --help" for details)
          |${config.systemHelp}
      """.stripMargin
    error.foreach(m => logger.error(m))
    println(usage)
  }

  val RESOURCE_PATTERN: Regex = "resource://(.*)".r

  /**
   * Generates a timestamp and watermark assigner for a stream with a given
   * type of element that limits how late an element is allowed to arrive
   * in event time.
   *
   * @tparam E
   *   the type of stream element
   * @return
   *   BoundedLatenessGenerator[E]
   */
  def boundedLatenessWatermarks[E <: ADT: TypeInformation](
      streamID: String
  ) =
    new BoundedLatenessWatermarkStrategy[E](
      config.maxLateness,
      streamID
    )

  /**
   * Create a bounded of order watermark strategy with idleness checking
   *
   * @tparam E
   *   the type of stream element
   * @return
   *   BoundedLatenessGenerator[E]
   */
  def boundedOutOfOrderWatermarks[E <: ADT: TypeInformation]()
      : WatermarkStrategy[E] =
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
  def ascendingTimestampsWatermarks[E <: ADT: TypeInformation]()
      : WatermarkStrategy[E] = WatermarkStrategy.forMonotonousTimestamps()

  /**
   * Assign timestamps/watermarks if we're using event time
   * @param in
   *   the input stream to watermark
   * @tparam E
   *   event type
   * @return
   *   the possibly watermarked input stream
   */
  def maybeAssignTimestampsAndWatermarks[E <: ADT: TypeInformation](
      in: DataStream[E],
      srcConfig: SourceConfig
  ): DataStream[E] =
    in.assignTimestampsAndWatermarks(srcConfig.watermarkStrategy match {
      case "bounded out of order" =>
        boundedOutOfOrderWatermarks()
      case "ascending timestamps" => ascendingTimestampsWatermarks()
      case _                      => boundedLatenessWatermarks(in.name)
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
  def fromSource[E <: ADT: TypeInformation](
      sourceName: String = ""
  ): DataStream[E] = {
    val name   =
      if (sourceName.isEmpty) config.getSourceNames.head else sourceName
    val src    = config.getSourceConfig(name)
    val uid    = src.label
    val stream = (src match {
      case src: KafkaSourceConfig      => fromKafka(src)
      case src: KinesisSourceConfig    => fromKinesis(src)
      case src: RabbitMQSourceConfig   => fromRabbitMQ(src)
      case src: FileSourceConfig       => fromFile(src)
      case src: SocketSourceConfig     => fromSocket(src)
      case src: CollectionSourceConfig => fromCollection(src)
    }).name(uid).uid(uid)
    maybeAssignTimestampsAndWatermarks(stream, src)
  }

  /**
   * Configure stream from kafka source.
   *
   * @param srcConfig
   *   a source config
   * @tparam E
   *   stream element type
   * @return
   *   DataStream[E]
   */
  def fromKafka[E <: ADT: TypeInformation](
      srcConfig: KafkaSourceConfig
  ): DataStream[E] = {
    val ksb             = KafkaSource
      .builder[E]()
      .setProperties(srcConfig.properties)
      .setStartingOffsets(srcConfig.startingOffsets)
      .setDeserializer(
        config
          .getKafkaRecordDeserializationSchema[E](
            srcConfig.name
          )
      )
    val kafkaSrcBuilder =
      if (srcConfig.bounded) ksb.setBounded(srcConfig.stoppingOffsets)
      else ksb
    env
      .fromSource(
        kafkaSrcBuilder.build(),
        srcConfig.watermarkStrategy match {
          case "bounded out of order" =>
            boundedOutOfOrderWatermarks[E]()
          case "ascending timestamps" => ascendingTimestampsWatermarks[E]()
          case _                      => boundedLatenessWatermarks[E](srcConfig.name)
        },
        srcConfig.label
      )
  }

  /**
   * Configure stream from kinesis.
   *
   * @param srcConfig
   *   a source config
   * @tparam E
   *   stream element type
   * @return
   *   DataStream[E]
   */
  def fromKinesis[E <: ADT: TypeInformation](
      srcConfig: KinesisSourceConfig
  ): DataStream[E] =
    env.addSource(
      new FlinkKinesisConsumer[E](
        srcConfig.stream,
        config
          .getKinesisDeserializationSchema[E](srcConfig.name),
        srcConfig.properties
      )
    )

  /**
   * Configure stream from file source.
   *
   * @param srcConfig
   *   a source config
   * @tparam E
   *   stream element type
   * @return
   *   DataStream[E]
   */
  def fromFile[E <: ADT: TypeInformation](
      srcConfig: FileSourceConfig
  ): DataStream[E] = {
    val path = srcConfig.path match {
      case RESOURCE_PATTERN(p) => getSourceFilePath(p)
      case other               => other
    }
    val ds   = config
      .getDeserializationSchema[E](srcConfig.name)
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
   * @tparam E
   *   stream element type
   * @return
   *   DataStream[E]
   */
  def fromSocket[E <: ADT: TypeInformation](
      srcConfig: SocketSourceConfig
  ): DataStream[E] =
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
   * @tparam E
   *   stream element type
   * @return
   *   DataStream[E]
   */
  def fromCollection[E <: ADT: TypeInformation](
      srcConfig: CollectionSourceConfig
  ): DataStream[E] =
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
   * Configure a stream from rabbitmq.
   * @param srcConfig
   *   a RabbitMQSourceConfig instance
   * @tparam E
   *   instance type of flink runner ADT
   * @return
   *   DataStream[E]
   */
  def fromRabbitMQ[E <: ADT: TypeInformation](
      srcConfig: RabbitMQSourceConfig): DataStream[E] = {
    val name                  = srcConfig.name
    val connConfig            = srcConfig.connectionInfo.rmqConfig
    val deserializationSchema = config.getRMQDeserializationSchema[E](name)
    env
      .addSource(
        new RMQSource(
          connConfig,
          srcConfig.queue,
          srcConfig.useCorrelationId,
          deserializationSchema
        )
      )
      .setParallelism(1) // required to get exactly once semantics
  }

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

  val runner: FlinkRunner[ADT] = this

  implicit class EventStreamOps[E <: ADT: TypeInformation](
      stream: DataStream[E]) {

    def as[T <: ADT: TypeInformation]: DataStream[T] = {
      val name = stream.name
      stream
        .filter((e: E) => e.isInstanceOf[T @unchecked])
        .name(s"filter types $name")
        .uid(s"filter types $name")
        .map((e: E) => e.asInstanceOf[T @unchecked])
        .name(s"cast types $name")
        .uid(s"cast types $name")
    }

    def toSink(sinkName: String = ""): Object =
      runner.toSink[E](stream, sinkName)

  }

  /**
   * Configure stream sink from configuration.
   *
   * @param stream
   *   the data stream to send to sink
   * @param sinkName
   *   a sink name to obtain configuration
   * @tparam E
   *   stream element type
   * @return
   *   DataStream[E]
   */
  def toSink[E <: ADT: TypeInformation](
      stream: DataStream[E],
      sinkName: String = ""
  ): Object = {
    val name       = if (sinkName.isEmpty) config.getSinkNames.head else sinkName
    val sinkConfig = config.getSinkConfig(name)
    val label      = sinkConfig.label
    sinkConfig match {
      case s: KafkaSinkConfig         =>
        toKafka[E](stream, s).uid(label).name(label)
      case s: KinesisSinkConfig       =>
        toKinesis[E](stream, s).uid(label).name(label)
      case s: RabbitMQSinkConfig      => toRabbitMQ[E](stream, s)
      case s: FileSinkConfig          => toFile[E](stream, s).uid(label).name(label)
      case s: SocketSinkConfig        =>
        toSocket[E](stream, s).uid(label).name(label)
      case s: JdbcSinkConfig          => toJdbc[E](stream, s).uid(label).name(label)
      case s: CassandraSinkConfig     =>
        toCassandraSink[E](stream, s).uid(label).name(label)
      case s: ElasticsearchSinkConfig =>
        toElasticsearchSink[E](stream, s).uid(label).name(label)
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
   * @tparam E
   *   stream element type
   * @return
   *   DataStreamSink[E]
   */
  def toKafka[E <: ADT: TypeInformation](
      stream: DataStream[E],
      sinkConfig: KafkaSinkConfig
  ): DataStreamSink[E] =
    stream
      .sinkTo(
        KafkaSink
          .builder()
          .setKafkaProducerConfig(sinkConfig.properties)
          .setRecordSerializer(
            config
              .getKafkaRecordSerializationSchema[E](
                sinkConfig.name
              )
          )
          .build()
      )

  /**
   * Send stream to a kinesis sink.
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
  def toKinesis[E <: ADT: TypeInformation](
      stream: DataStream[E],
      sinkConfig: KinesisSinkConfig
  ): DataStreamSink[E] =
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

  /**
   * A jdbc sink.
   * @param stream
   *   a data stream
   * @param sinkConfig
   *   a JdbcSinkConfig object
   * @tparam E
   *   the type of elements in the data stream
   * @return
   *   DataStreamSink
   */
  def toJdbc[E <: ADT: TypeInformation](
      stream: DataStream[E],
      sinkConfig: JdbcSinkConfig
  ): DataStreamSink[E] = {
    val sinkProps              = sinkConfig.properties
    val addToJdbcBatchFunction =
      config.getAddToJdbcBatchFunction[E](sinkConfig.name)
    val statementBuilder = {
      new JdbcStatementBuilder[E] {
        override def accept(ps: PreparedStatement, element: E): Unit =
          addToJdbcBatchFunction.addToJdbcStatement(element, ps)
      }
    }
    val executionOptions       = JdbcExecutionOptions
      .builder()
      .withMaxRetries(
        Option(sinkProps.getProperty("max.retries"))
          .map(o => o.toInt)
          .getOrElse(JdbcExecutionOptions.DEFAULT_MAX_RETRY_TIMES)
      )
      .withBatchSize(
        Option(sinkProps.getProperty("batch.size"))
          .map(o => o.toInt)
          .getOrElse(JdbcExecutionOptions.DEFAULT_SIZE)
      )
      .withBatchIntervalMs(
        Option(sinkProps.getProperty("batch.interval.ms"))
          .map(o => o.toLong)
          .getOrElse(60L)
      )
      .build()
    val connectionOptions      =
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl(sinkConfig.url)
        .build()
    stream.addSink(
      JdbcSink.sink(
        sinkConfig.query,
        statementBuilder,
        executionOptions,
        connectionOptions
      )
    )
  }

  /**
   * Send stream to a rolling file sink.
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
  def toFile[E <: ADT: TypeInformation](
      stream: DataStream[E],
      sinkConfig: FileSinkConfig
  ): DataStreamSink[E] = {
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
    stream.addSink(sink)
  }

  /**
   * Send stream to a socket sink.
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
  def toSocket[E <: ADT: TypeInformation](
      stream: DataStream[E],
      sinkConfig: SocketSinkConfig
  ): DataStreamSink[E] =
    stream
      .writeToSocket(
        sinkConfig.host,
        sinkConfig.port,
        config
          .getSerializationSchema(sinkConfig.name)
          .asInstanceOf[SerializationSchema[E]]
      )

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
  def toCassandraSink[E <: ADT: TypeInformation](
      stream: DataStream[E],
      sinkConfig: CassandraSinkConfig): CassandraSink[E] =
    CassandraSink
      .addSink(stream)
      .setHost(sinkConfig.host)
      .setQuery(sinkConfig.query)
      .build()

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
  def toElasticsearchSink[E <: ADT: TypeInformation](
      stream: DataStream[E],
      sinkConfig: ElasticsearchSinkConfig
  ): DataStreamSink[E] = {
    val hosts         = sinkConfig.transports.map { s =>
      val url      = new URL(if (s.startsWith("http")) s else s"http://$s")
      val hostname = url.getHost
      val port     = if (url.getPort < 0) 9200 else url.getPort
      new HttpHost(hostname, port, url.getProtocol)
    }.asJava
    val esSinkBuilder = new ElasticsearchSink.Builder[E](
      hosts,
      (element: E, _: RuntimeContext, indexer: RequestIndexer) => {
        val data = element.getClass.getDeclaredFields
          .filterNot(f => f.getName.startsWith("$"))
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
    )

    val props = sinkConfig.properties
    Option(props.getProperty("bulk.flush.backoff.enable"))
      .map(_.toBoolean)
      .foreach(esSinkBuilder.setBulkFlushBackoff)
    Option(props.getProperty("bulk.flush.backoff.type"))
      .map(_.toUpperCase() match {
        case "CONSTANT"    => FlushBackoffType.CONSTANT
        case "EXPONENTIAL" => FlushBackoffType.EXPONENTIAL
        case t             =>
          logger.warn(
            s"invalid bulk.flush.backoff.type value '$t'; using CONSTANT"
          )
          FlushBackoffType.CONSTANT
      })
      .foreach(esSinkBuilder.setBulkFlushBackoffType)
    Option(
      props.getProperty("bulk.flush.backoff.delay")
    ).map(_.toLong)
      .foreach(esSinkBuilder.setBulkFlushBackoffDelay)
    Option(props.getProperty("bulk.flush.backoff.retries"))
      .map(_.toInt)
      .foreach(esSinkBuilder.setBulkFlushBackoffRetries)
    Option(props.getProperty("bulk.flush.max.actions"))
      .map(_.toInt)
      .foreach(esSinkBuilder.setBulkFlushMaxActions)
    Option(props.getProperty("bulk.flush.max.size.mb"))
      .map(_.toInt)
      .foreach(esSinkBuilder.setBulkFlushMaxSizeMb)
    Option(props.getProperty("bulk.flush.interval.ms"))
      .map(_.toLong)
      .foreach(esSinkBuilder.setBulkFlushInterval)
    stream.addSink(esSinkBuilder.build())
  }

  /**
   * Configure streaming to a rabbitmq sink.
   * @param stream
   *   the stream to send to the sink
   * @param sinkConfig
   *   a RabbitMQSinkConfig instance
   * @tparam E
   *   type of the flink runner ADT events in the stream
   * @return
   *   DataStreamSink[E]
   */
  def toRabbitMQ[E <: ADT: TypeInformation](
      stream: DataStream[E],
      sinkConfig: RabbitMQSinkConfig
  ): DataStreamSink[E] = {

    val name                = sinkConfig.name
    val connConfig          = sinkConfig.connectionInfo.rmqConfig
    val serializationSchema =
      config.getSerializationSchema[E](sinkConfig.name)

    stream.addSink(
      config.getRabbitPublishOptions[E](sinkConfig.name) match {
        case Some(p) => new RMQSink(connConfig, serializationSchema, p)
        case None    =>
          sinkConfig.queue match {
            case Some(q) => new RMQSink(connConfig, q, serializationSchema)
            case None    =>
              throw new RuntimeException(
                s"RabbitMQ config requires either a queue name or publishing options for sink $name"
              )
          }
      }
    )
  }

}
