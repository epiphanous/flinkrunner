<h1 align="center">FlinkRunner</h1>

<div align="center">
  <strong>A scala library to simplify writing Flink Datastream API jobs</strong>
</div>

<div align="center">
<br />
<!-- license -->
<a href="https://github.com/epiphanous/flinkrunner/blob/master/LICENSE" title="License">
  <img src="https://img.shields.io/badge/license-MIT-brightgreen.svg" alt="license"/>
</a>
<!-- release -->
<a href="https://github.com/epiphanous/flinkrunner/releases" title="release">
  <img src="https://img.shields.io/github/release/epiphanous/flinkrunner.svg" alt="release" />
</a>
<!-- maven central -->
<a href="https://mvnrepository.com/artifact/io.epiphanous/flinkrunner">
  <img src="https://img.shields.io/maven-central/v/io.epiphanous/flinkrunner_2.12.svg" alt="maven" />
</a>
<!-- last commit -->
<a href="https://github.com/epiphanous/flinkrunner/commits" title="Last Commit">
  <img src="https://img.shields.io/github/last-commit/epiphanous/flinkrunner.svg" alt="commit" />
</a>
<!-- build -->
<a href="https://travis-ci.com/epiphanous/flinkrunner" title="Build Status">
  <img src="https://img.shields.io/travis/com/epiphanous/flinkrunner.svg" alt="build" />
</a>
<!-- coverage -->
<a href='https://coveralls.io/github/epiphanous/flinkrunner?branch=master'><img src='https://coveralls.io/repos/github/epiphanous/flinkrunner/badge.svg?branch=master' alt='Coverage Status' /></a>
</div>

<div align="center">
  <sub>Built by
  <a href="https://twitter.com/epiphanous">nextdude@epiphanous.io</a> and
  <a href="https://github.com/epiphanous/flinkrunner/graphs/contributors">
    contributors
  </a></sub>
</div>

## Dependencies

`Flinkrunner 4`
is [available on maven central](https://mvnrepository.com/artifact/io.epiphanous/flinkrunner_2.12)
, built against Flink 1.15 with Scala 2.12 and JDK 11. You can add it to your sbt project:

```sbtshell
libraryDependencies += "io.epiphanous" %% "flinkrunner" % <flinkrunner-version>
```

replacing `<flinkrunner-version>` with the currently released version
of [`Flinkrunner` on maven](https://mvnrepository.com/artifact/io.epiphanous/flinkrunner_2.12)
.

### Connectors

Flinkrunner is built to support many common data sources and sinks, including:

| Connector     | Dependency                     | Source | Sink |
|:--------------|:-------------------------------|:------:|:----:|
| file system   | flink-connector-files          |  yes   | yes  |
| kafka         | flink-connector-kafka          |  yes   | yes  |
| kinesis       | flink-connector-kinesis        |  yes   | yes  |
| cassandra     | flink-connector-cassandra      |   no   | yes  |
| elasticsearch | flink-connector-elasticsearch7 |   no   | yes  |
| rabbit mq     | flink-connector-rabbitmq       |  yes   | yes  |

You can add a dependency for a connector you want to use by dropping the library into
flink's`lib` directory during deployment of your jobs. You should make sure to match the
library's version with the compiled flink and scala versions of `FlinkRunner`.

To run tests locally in your IDE, you can add a connector library to your dependencies
like this:

```
"org.apache.flink" % "flink-connector-kafka" % <flink-version> % Provided
```

replacing `<flink-version>` with the version of flink used in `FlinkRunner`.

### S3 Support

S3 configuration is important for most flink usage scenarios. Flink has two different
implementations to support S3:
`flink-s3-fs-presto` and `flink-s3-fs-hadoop`.

* `flink-s3-fs-presto` is registered under the schemes `s3://` and `s3p://` and is
  preferred for checkpointing to s3.
* `flink-s3-fs-hadoop` is registered under the schemes `s3://` and `s3a://` and is
  required for using the streaming file sink.

During deployment, you should copy both s3 dependencies from flink's `opt` directory into
the `plugins` directory:

```bash
cd $FLINK_DIR
mkdir -p ./plugins/s3
cp ./opt/flink-s3-fs-presto-1.14.3.jar .plugins/s3-fs-preso
cp ./opt/flink-s3-fs-hadoop-1.14.3.jar .plugins/s3-fs-hadoop
```

> *NOTE*:  Do not copy them into flink's `lib` directory, as this will not work! They need
> to be in their own,
> individual subdirectories of flink's deployed `plugins` directory.

> *NOTE
> Further, you'll need
>
to [configure access](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/deployment/filesystems/s3/#configure-access-credentials)
> from your job to AWS S3. That is outside the scope of this readme.

### Avro Support

Add the following dependencies if you need Avro support:

```
"org.apache.flink" % "flink-avro"                     % <flink-version>,
"org.apache.flink" % "flink-avro-confluent-registry"  % <flink-version>,
"io.confluent"     % "kafka-avro-serializer"          % <confluent-version>
```

Note, `kafka-avro-serializer` requires you add an sbt resolver to your
project's `build.sbt` file to point at Confluent's maven repository:

```
resolvers += "Confluent Repository" at "https://packages.confluent.io/maven/"
```

Adding the `kafka-avro-serializer` library to your project will enable Confluent schema
registry support.

#### Schema Registry Support

Flinkrunner provides support for serializing and deserializing in and out of kafka using
Confluent's [Avro schema registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
. To make use of this support, you need to do three things:

1. Iinclude the `kafka-avro-serializer` library in your project (as described above).
2. Configure your kafka sources and sinks with a `schema.registry` block like so:

```hocon
sinks {
  kafka-sink {
    bootstrap.servers = "kafka:9092"
    topic = my-topic
    schema-registry {
      url = "http://schema-registry:8082" // required
      // other settings are possible
      //cache.capacity = 500 // (1000 by default)
      //props {
      // --- This optional section holds any confluent configs to pass to 
      // --- KafkaAvroSerde schema registry classes
      //use.logical.type.converters = false // (true by default)
      //specific.avro.reader = false        // (true by default)
      //}
    }
  }
}
```

3. Use the `AvroStreamJob` base class for your flink jobs. This is described in more
   detail below.

### Parquet Support

To enable file sinks to write in parquet format, add the following dependency to your
build:

```
"org.apache.parquet" % "parquet-avro" % 1.12.3
```

Add use the `format = parquet` directive in your file sink configuration (more details
below).

### Logging

`Flinkrunner` uses [scala-logging](https://github.com/lightbend/scala-logging) for logging
on top of slf4j. In your implementation, you must provide a logging backend compatible
with slf4j, such as [logback](https://logback.qos.ch/):

```sbtshell
libraryDependencies +=   "ch.qos.logback" % "logback-classic" % "1.2.10"
```

### Complex Event Processing

If you want to use the complex event processing library, add this dependency:

```
"org.apache.flink" %% "flink-cep-scala" % <flink-version>
```

## What is FlinkRunner?

`FlinkRunner` helps you think about your flink jobs at a high level, so you can focus on
the event pipeline, not the plumbing.

You have a set of related flink jobs that deal in a related set of data event
types. `Flinkrunner` helps you build one application to run those related jobs and
coordinate the types. It also simplifies setting up common sources and sinks to the point
where you control them purely with configuration and not code. `Flinkrunner` supports a
variety of sources and sinks out of the box, including `kafka`, `kinesis`, `jdbc`
, `elasticsearch 7+` (sink only), `cassandra` (sink only),
`filesystems` (including `s3` and `parquet`) and `sockets`. It also has many common
operators to help you in writing your own transformation logic. Finally, `FlinkRunner`
makes it easy to test your transformation logic with property-based testing.

## Get Started

* Define an
  [*algebraic data
  type*](http://tpolecat.github.io/presentations/algebraic_types.html#1) (ADT) containing
  the types that will be used in your flink jobs. Your top level type should inherit
  from `FlinkEvent`. This will force your types to implement the following methods/fields:

  * `$timestamp: Long` - the event time
  * `$id: String` - a unique id for the event
  * `$key: String` - a key to group events

Additionally, a `FlinkEvent` has an `$active: Boolean` method that defaults to `false`.
This is used by `FlinkRunner` in some of its abstract job types that you may or may not
use. We'll discuss those details later.

  ```scala
  sealed trait MyADTEvent extends FlinkEvent

final case class SomeEvent($timestamp: Long, $id: String, $key: String,

...)
extends MyADTEvent

final case class OtherEvent($timestamp: Long, $id: String, $key: String,

...)
extends MyADTEvent
  ```

* Create a `Runner` object using `FlinkRunner`. This is what you use as the entry point to
  your jobs.

  ```scala

object Runner {

def main(args: Array[String]): Unit = run(args)

def run(
args: Array[String], optConfig: Option[String] = None, sources: Map[String, Seq[
Array[Byte]]] = Map.empty
)(callback: PartialFunction[Stream[MyADTEvent], Unit] = { case _ =>
()
}): Unit = { val runner = new FlinkRunner[MyADTEvent](
args, new MyADTRunnerFactory(), sources, optConfig
)
runner.process(callback)
} }

  ```

* Create a `MyADTRunnerFactory()` that extends `FlinkRunnerFactory()`. This is used
by `FlinkRunner` to get your jobs and serialization/deserialization schemas.

  ```scala
@SerialVersionUID(123456789L)
class MyADTRunnerFactory
    extends FlinkRunnerFactory[MyADTEvent]
    with Serializable {

  override def getJobInstance(name: String) =
    name match {
      case "MyADTJob1" => new MyADTJob1()
      case "MyADTJob2" => new MyADTJob2()
      case "MyADTJob3" => new MyADTJob3()
      case _           =>
        throw new UnsupportedOperationException(s"unknown job $name")
    }

  // just override the ones you need in your jobs
  // note these are intended to work for all your types, which is one of the
  // primary reasons we're using an ADT approach
  override def getDeserializationSchema =
    new MyADTEventDeserializationSchema()

  override def getKeyedDeserializationSchema =
    new MyADTEventKeyedDeserializationSchema()

  override def getSerializationSchema =
    new MyADTEventSerializationSchema()

  override def getKeyedSerializationSchema =
    new MyADTEventKeyedSerializationSchema()

  override def getEncoder = new MyADTEventEncoder()

  override def getAddToJdbcBatchFunction =
    new MyADTEventAddToJdbcBatchFunction()
}

  ```

* Create your serialization and deserialization schemas for your ADT. You'll most likely
  need a deserialization schema for your source and either a serialization schema, encoder
  or jdbc batch adder for your sink. These are simple functions that extend simple
  existing interfaces. These functions are also simplified by the fact that you've already
  organized your data as an ADT.

* Configure your system, which mainly is about configuring sources and
  sinks. `Flinkrunner`
  uses [typesafe config](https://lightbend.github.io/config/) for its configuration,
  overlaid with command line arguments and optionally augmented with options you pass in
  programmatically at run time.
  `Flinkrunner` automatically merges this info for you, so you don't have to parse command
  line arguments and create `ParameterTool` objects. All of your job methods will be
  passed an implicit `FlinkConfig` object that gives you access to the current
  configuration. Further, `FlinkRunner` takes care of configuring your streaming
  environment automatically based on the configuration settings. Perhaps the best part is
  you don't have to write any code at all to setup sources and sinks, as these are all
  grokked by `FlinkRunner` from the config.

* Write your jobs! This is the fun part. You job will inherit from one of `FlinkRunner`'s
  job classes. The top level `FlinkRunner` job class is called `BaseFlinkJob` and has the
  following interface:

```scala
/**
  * An abstract flink job to transform on an input stream into an output
  * stream.
  *
  * @tparam DS
  * The type of the input stream
  * @tparam OUT
  * The type of output stream elements
  */
abstract class BaseFlinkJob[DS, OUT <: FlinkEvent : TypeInformation]
  extends LazyLogging {

  /**
    * A pipeline for transforming a single stream. Passes the output of
    * [[source()]] through [[transform()]] and the result of that into
    * [[maybeSink()]], which may pass it into [[sink()]] if we're not
    * testing. Ultimately, returns the output data stream to facilitate
    * testing.
    *
    * @param config
    * implicit flink job config
    * @return
    * data output stream
    */
  def flow()(implicit
             config: FlinkConfig,
             env: StreamExecutionEnvironment): DataStream[OUT] =
    source |> transform |# maybeSink

  def run()(implicit
            config: FlinkConfig,
            env: StreamExecutionEnvironment): Either[Iterator[OUT], Unit] = {

    logger.info(
      s"\nSTARTING FLINK JOB: ${config.jobName} ${config.jobArgs.mkString(" ")}\n"
    )

    val stream = flow

    if (config.showPlan) logger.info(s"PLAN:\n${env.getExecutionPlan}\n")

    if (config.mockEdges)
      Left(DataStreamUtils.collect(stream.javaStream).asScala)
    else
      Right(env.execute(config.jobName))
  }

  /**
    * Returns source data stream to pass into [[transform()]]. This must be
    * overridden by subclasses.
    * @return
    * input data stream
    */
  def source()(implicit
               config: FlinkConfig,
               env: StreamExecutionEnvironment): DS

  /**
    * Primary method to transform the source data stream into the output
    * data stream. The output of this method is passed into [[sink()]]. This
    * method must be overridden by subclasses.
    *
    * @param in
    * input data stream created by [[source()]]
    * @param config
    * implicit flink job config
    * @return
    * output data stream
    */
  def transform(in: DS)(implicit
                        config: FlinkConfig,
                        env: StreamExecutionEnvironment): DataStream[OUT]

  /**
    * Writes the transformed data stream to configured output sinks.
    *
    * @param out
    * a transformed stream from [[transform()]]
    * @param config
    * implicit flink job config
    */
  def sink(out: DataStream[OUT])(implicit
                                 config: FlinkConfig,
                                 env: StreamExecutionEnvironment): Unit =
    config.getSinkNames.foreach(name => out.toSink(name))

  /**
    * The output stream will only be passed to [[sink()]] if
    * [[FlinkConfig.mockEdges]] evaluates to false (ie, you're not testing).
    *
    * @param out
    * the output data stream to pass into [[sink()]]
    * @param config
    * implicit flink job config
    */
  def maybeSink(out: DataStream[OUT])(implicit
                                      config: FlinkConfig,
                                      env: StreamExecutionEnvironment): Unit =
    if (!config.mockEdges) sink(out)

}

```

More commonly you'll extend from `FlinkJob[IN,OUT]` where `IN` and `OUT` are classes in
your ADT.

```scala
/**
  * An abstract flink job to transform on a stream of events from an algebraic data type (ADT).
  *
  * @tparam IN  The type of input stream elements
  * @tparam OUT The type of output stream elements
  */
abstract class FlinkJob[IN <: FlinkEvent : TypeInformation, OUT <: FlinkEvent : TypeInformation]
  extends BaseFlinkJob[DataStream[IN], OUT] {

  def getEventSourceName(implicit config: FlinkConfig) = config.getSourceNames.headOption.getOrElse("events")

  /**
    * Returns source data stream to pass into [[transform()]]. This can be overridden by subclasses.
    * @return input data stream
    */
  def source()(implicit config: FlinkConfig, env: StreamExecutionEnvironment): DataStream[IN] =
    fromSource[IN](getEventSourceName) |# maybeAssignTimestampsAndWatermarks

}
```

Where as `FlinkJob` requires your input stream to be a `DataStream[IN]`, `BaseFlinkJob`
let's you set the type of input stream, allowing you to use other, more complicated stream
types in Flink, such as `ConnectedStreams[IN1,IN2]`
or `BroadcastStream[B,D]`.

While you're free to override any of these methods in your job, usually you just need
extend `FlinkJob` and provide a `transform()`
method that converts your `DataStream[IN]` to a `DataStream[OUT]`.

> TODO: Finish README.
