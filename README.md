<h1 align="center">FlinkRunner</h1>

<div align="center">
  <strong>A scala library to simplify flink jobs</strong>
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
  </a>
</div>

## Maven Dependency

`Flinkrunner 3` is available on maven central, built against Flink 1.11 with Scala 2.12 and JDK 11.

```sbtshell
libraryDependencies += "io.epiphanous" %% "flinkrunner" % "3.0.5"
```

## What is FlinkRunner?

You have a set of related flink jobs that deal in a related set of
data event types. `Flinkrunner` helps you build one application to run
those related jobs and coordinate the types. It also simplifies setting
up common sources and sinks to the point where you control them purely
with configuration and not code. `Flinkrunner` supports a variety of sources
and sinks out of the box, including `kafka`, `kinesis`, `jdbc`, `elasticsearch 7+` (sink only),
`cassandra` (sink only),  `filesystems` (including `s3`) and `sockets`. It also has many common
operators to help you in writing your own transformation logic. Finally, `FlinkRunner`
makes it easy to test your transformation logic with property-based testing.

`FlinkRunner` helps you think about your flink jobs at a high level,
so you can focus on the event pipeline, not the plumbing.

## Get Started

* Define an
[*algebraic data type*](http://tpolecat.github.io/presentations/algebraic_types.html#1)
(ADT) containing the types that will be used in your flink jobs. Your top level type
should inherit from `FlinkEvent`. This will force your types to implement the following
methods/fields:

  * `$timestamp: Long` - the event time
  * `$id: String` - a unique id for the event
  * `$key: String` - a key to group events

  Additionally, a `FlinkEvent` has an `$active: Boolean` method that defaults to `false`.
  This is used by `FlinkRunner` in some of its abstract job types that you may or may not
  use. We'll discuss those details later.

  ```scala
  sealed trait MyADTEvent extends FlinkEvent

  final case class SomeEvent($timestamp:Long, $id:String, $key:String, ...)
    extends MyADTEvent

  final case class OtherEvent($timestamp:Long, $id:String, $key:String, ...)
    extends MyADTEvent
  ```

* Create a `Runner` object using `FlinkRunner`. This is what you use as the entry point
to your jobs.

  ```scala
  object Runner {

    def main(args: Array[String]): Unit =
      run(args)

    def run(
        args: Array[String],
        optConfig: Option[String] = None,
        sources: Map[String, Seq[Array[Byte]]] = Map.empty
      )(callback: PartialFunction[Stream[MyADTEvent], Unit] = {
          case _ =>
            ()
        }
      ): Unit = {
      val runner =
        new FlinkRunner[MyADTEvent](args,
                                          new MyADTRunnerFactory(),
                                          sources,
                                          optConfig)
      runner.process(callback)
    }
  }
  ```

* Create a `MyADTRunnerFactory()` that extends `FlinkRunnerFactory()`. This is used
by flink runner to get your jobs and serialization/deserialization schemas.

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
        case _ => throw new UnsupportedOperationException(s"unknown job $name")
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

* Create your serialization and deserialization schemas for your ADT.
You'll most likely need a deserialization schema
for your source and either a serialization schema, encoder or jdbc batch adder
for your sink. These are simple functions that extend simple existing interfaces.
These functions are also simplified by the fact that you've already organized your
data as an ADT.

* Configure your system, which mainly is about configuring sources and sinks. `Flinkrunner`
uses [typesafe config](https://lightbend.github.io/config/) for its configuration,
overlaid with command line arguments
and optionally augmented with options you pass in programmatically at run time.
`Flinkrunner` automatically merges this info for you, so you don't have
to parse command line arguments and create `ParameterTool` objects.
All of your job methods will be passed an implicit `FlinkConfig` object that
gives you access to
the current configuration. Further, `FlinkRunner` takes care of configuring your streaming
environment automatically based on the configuration settings. Perhaps the best part is
you don't have to write any code at all to setup sources and sinks, as these are all
grokked by `FlinkRunner` from the config.

* Write your jobs! This is the fun part. You job will inherit from one of `FlinkRunner`'s
job classes. The top level `FlinkRunner` job class is called `BaseFlinkJob` and has the
following interface:

```scala
/**
  * An abstract flink job to transform on an input stream into an output stream.
  *
  * @tparam DS   The type of the input stream
  * @tparam OUT  The type of output stream elements
  */
abstract class BaseFlinkJob[DS, OUT <: FlinkEvent: TypeInformation] extends LazyLogging {

  /**
    * A pipeline for transforming a single stream. Passes the output of [[source()]]
    * through [[transform()]] and the result of that into [[maybeSink()]], which may pass it
    * into [[sink()]] if we're not testing. Ultimately, returns the output data stream to
    * facilitate testing.
    *
    * @param config implicit flink job config
    * @return data output stream
    */
  def flow()(implicit config: FlinkConfig, env: SEE): DataStream[OUT] =
    source |> transform |# maybeSink

  def run()(implicit config: FlinkConfig, env: SEE): Either[Iterator[OUT], Unit] = {

    logger.info(s"\nSTARTING FLINK JOB: ${config.jobName} ${config.jobArgs.mkString(" ")}\n")

    val stream = flow

    if (config.showPlan) logger.info(s"PLAN:\n${env.getExecutionPlan}\n")

    if (config.mockEdges)
      Left(DataStreamUtils.collect(stream.javaStream).asScala)
    else
      Right(env.execute(config.jobName))
  }

  /**
    * Returns source data stream to pass into [[transform()]]. This must be overridden by subclasses.
    * @return input data stream
    */
  def source()(implicit config: FlinkConfig, env: SEE): DS

  /**
    * Primary method to transform the source data stream into the output data stream. The output of
    * this method is passed into [[sink()]]. This method must be overridden by subclasses.
    *
    * @param in input data stream created by [[source()]]
    * @param config implicit flink job config
    * @return output data stream
    */
  def transform(in: DS)(implicit config: FlinkConfig, env: SEE): DataStream[OUT]

  /**
    * Writes the transformed data stream to configured output sinks.
    *
    * @param out a transformed stream from [[transform()]]
    * @param config implicit flink job config
    */
  def sink(out: DataStream[OUT])(implicit config: FlinkConfig, env: SEE): Unit =
    config.getSinkNames.foreach(name => out.toSink(name))

  /**
    * The output stream will only be passed to [[sink()]] if [[FlinkConfig.mockEdges]] evaluates
    * to false (ie, you're not testing).
    *
    * @param out the output data stream to pass into [[sink()]]
    * @param config implicit flink job config
    */
  def maybeSink(out: DataStream[OUT])(implicit config: FlinkConfig, env: SEE): Unit =
    if (!config.mockEdges) sink(out)

}
```

More commonly you'll extend from `FlinkJob[IN,OUT]` where `IN` and `OUT` are classes in your ADT.

```scala
/**
  * An abstract flink job to transform on a stream of events from an algebraic data type (ADT).
  *
  * @tparam IN   The type of input stream elements
  * @tparam OUT  The type of output stream elements
  */
abstract class FlinkJob[IN <: FlinkEvent: TypeInformation, OUT <: FlinkEvent: TypeInformation]
    extends BaseFlinkJob[DataStream[IN], OUT] {

  def getEventSourceName(implicit config: FlinkConfig) = config.getSourceNames.headOption.getOrElse("events")

  /**
    * Returns source data stream to pass into [[transform()]]. This can be overridden by subclasses.
    * @return input data stream
    */
  def source()(implicit config: FlinkConfig, env: SEE): DataStream[IN] =
    fromSource[IN](getEventSourceName) |# maybeAssignTimestampsAndWatermarks

}
```

Where as `FlinkJob` requires your input stream to be a `DataStream[IN]`, `BaseFlinkJob` let's you set the type of
input stream, allowing you to use other, more complicated stream types in Flink, such as `ConnectedStreams[IN1,IN2]` or `BroadcastStream[B,D]`.

While you're free to override any of these methods in your job, usually you just need extend `FlinkJob` and provide a `transform()`
method that converts your `DataStream[IN]` to a `DataStream[OUT]`.

> TODO: Finish README.

