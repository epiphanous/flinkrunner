<h1 align="center">FlinkRunner</h1>
<div align="center">
  <strong>A scala library to simplify flink jobs</strong>
</div>

<div align="center">
<!-- license -->
![license:MIT](https://img.shields.io/badge/license-MIT-brightgreen.svg)
<!-- release -->
![release](https://img.shields.io/github/release/epiphanous/flinkrunner.svg)
<!-- maven central -->
![maven-central](https://img.shields.io/maven-central/v/io.epiphanous/flinkrunner.svg)
<!-- last commit -->
![last-commit](https://img.shields.io/github/last-commit/epiphanous/flinkrunner.svg)
<!-- build -->
![build](https://img.shields.io/travis/com/epiphanous/flinkrunner.svg)
<!-- coverage -->
![Coverage Status](https://coveralls.io/repos/github/epiphanous/flinkrunner/badge.svg?branch=master)
</div>


## Usage

`Flinkrunner` is not on maven central yet (will be soon), so you have to build it yourself from source. If you
want AWS Kinesis support, you will have to build `flinkrunner` yourself even when `flinkrunner` is on maven central.

To build from source: 

* Clone this repo:

    ```bash
    git clone https://github.com/epiphanous/flinkrunner
    ```

* Checkout the tag of flinkrunner you want to build. The most recent stable version is
  `v1.5.1`, but you can ensure you have the most recent tags with `git fetch --tags` and 
  list tags with `git tag -l`, then
  
    ```bash
    git checkout tags/v1.5.1 -b my-build-v1.5.1
    ```
    
   This will create a new local branch `my-build-v1.5.1` based on the `v1.5.1` tag release.
      
* Build flinkrunner and install it locally, using the `--with-kinesis=true` option if you want
  to include support for AWS Kinesis in the library

    ```bash
    sbt [--with.kinesis=true] publishLocal
    ```
    
  This will install a local copy of version `v1.5.1` of `flinkrunner` in your local repo.

* In your project's build file, add a resolver to your local repo and add the local
  `flinkrunner` dependency:

    ```sbtshell
    resolvers += "Local Maven Repository" at "file://" +
        Path.userHome.absolutePath + "/.m2/repository" 
    ...
    libraryDependencies += "io.epiphanous" %% "flinkrunner" % "1.5.1"
                                      // notice no v here ----^^
    ```
  > If you don't add the local resolver, sbt will try to load the library from maven central, which
  may work in the future, but won't include kinesis under any circumstance.
  
## Kinesis

The apache flink project doesn't include its kinesis connector on maven
central because of license restrictions, and we don't include it in flinkrunner for the same reasons.
Thus in order to use kinesis, you first have to build flink's kinesis connector (see 
[these instructions](https://ci.apache.org/projects/flink/flink-docs-stable/dev/connectors/kinesis.html)),
then use the `--with-kinesis=true` option as described above when building your local copy of `FlinkRunner`.

> Note that building Flink with Kinesis can take over 30 minutes! It's a very big project.


## What is FlinkRunner?

You have a set of related flink jobs that deal in a related set of
data event types. `Flinkrunner` helps you build one application to run
those related jobs and coordinate the types. It also simplifies setting
up common sources and sinks to the point where you control them purely
with configuration and not code. `Flinkrunner` supports a variety of sources
and sinks out of the box, including `kafka`, `kinesis`, `jdbc`, `filesystems`
(including `s3`) and `sockets`. It also has many common
operators to help you in writing your own transformation logic. Finally, `flinkrunner`
makes it easy to test your transformation logic with property-based testing.

At a high level, `flinkrunner` helps you think about your flink jobs at a high level,
so you can focus on the logic in the pipeline between the sources and sinks.

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
the current configuration. Further, `flinkrunner` takes care of configuring your streaming
environment automatically based on the configuration settings. Perhaps the best part is
you don't have to write any code at all to setup sources and sinks, as these are all
grokked by `flinkrunner` from the config.

* Write your jobs! This is the fun part. You job will inherit from one of `flinkrunner`'s
job classes. The top level `flinkrunner` job class is called `FlinkJob` and has the
following interface:

  ```scala
  abstract class FlinkJob[IN <: FlinkEvent: TypeInformation, OUT <: FlinkEvent: TypeInformation] extends LazyLogging {

    // the entry point called by flink runner; invokes the flow
    // and if mock.edges is true, returns an iterator of results
    def run():Either[Iterator[OUT],Unit]

    // this is its actual code...sets up the execution plan
    def flow():DataStream[OUT] =
      source |> transform |# maybeSink

    // reads from first configured sink and assigns timestamps
    // and watermarks if needed
    def source():DataStream[IN]

    // called by source(), uses configuration to do its thing
    def maybeAssignTimestampsAndWatermarks(in: DataStream[IN]):Unit

    // writes to first configured sink
    def sink(out:DataStream[OUT]):Unit

    // only adds the sink to the execution plan of mock.edges is fals
    def maybeSink():Unit

    // no implementation provided...
    // this is what your job implements
    def transform(in:DataStream[IN]):DataStream[OUT]
  }
  ```

  While you're free to override any of these methods in your job,
  usually you just need to provide a `transform()` method that
  converts your `DataStream[IN]` to a `DataStream[OUT]`.

> TODO: Finish README.

