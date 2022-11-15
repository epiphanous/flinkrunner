<h1 align="center">FlinkRunner</h1>

<div align="center">
  <strong>A scala library to simplify writing Flink Datastream API jobs</strong>
</div>

<div align="center">
<br />
<!-- license -->
<a href="https://github.com/epiphanous/flinkrunner/blob/main/LICENSE" title="License">
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
<a href='https://coveralls.io/github/epiphanous/flinkrunner?branch=main'><img src='https://coveralls.io/repos/github/epiphanous/flinkrunner/badge.svg?branch=main' alt='Coverage Status' /></a>
</div>

<div align="center">
  <sub>Built by
  <a href="https://twitter.com/epiphanous">nextdude</a> and
  <a href="https://github.com/epiphanous/flinkrunner/graphs/contributors">
    contributors
  </a></sub>
</div>

## Dependencies

`Flinkrunner 4`
is [available on maven central](https://mvnrepository.com/artifact/io.epiphanous/flinkrunner_2.12)
, built against Flink 1.16 with Scala 2.12 and JDK 11. You can add it to your sbt project:

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
| jdbc          | flink-connector-jdbc           |   no   | yes  |
| rabbit mq     | flink-connector-rabbitmq       |  yes   | yes  |

You can add a dependency for a connector by dropping the library into flink's `lib`
directory during deployment of your jobs. You should make sure to match the library's
version with the compiled flink and scala versions of `FlinkRunner`.

To run tests locally in your IDE, add a connector library to your dependencies in provided
scope, like this:

```
"org.apache.flink" % "flink-connector-kafka" % <flink-version> % Provided
```

replacing `<flink-version>` with the version of flink used in `FlinkRunner`.

### S3 Support

S3 configuration is important for most flink usage scenarios. Flink has two different
implementations to support S3: `flink-s3-fs-presto` and `flink-s3-fs-hadoop`.

* `flink-s3-fs-presto` is registered under the schemes `s3://` and `s3p://` and is
  preferred for checkpointing to s3.
* `flink-s3-fs-hadoop` is registered under the schemes `s3://` and `s3a://` and is
  required for using the streaming file sink.

During deployment, you should copy both s3 dependencies from flink's `opt` directory into
the `plugins` directory:

```bash
cd $FLINK_DIR
mkdir -p ./plugins/s3-fs-presto
cp ./opt/flink-s3-fs-presto-<flink-version>.jar .plugins/s3-fs-presto
mkdir -p ./plugins/s3-fs-hadoop
cp ./opt/flink-s3-fs-hadoop-<flink-version>.jar .plugins/s3-fs-hadoop
```

replacing `<flink-version>` with the version of flink used in `FlinkRunner`.

> *NOTE*:  Do not copy them into flink's `lib` directory, as this will not work! They need
> to be in their own, individual subdirectories of flink's deployed `plugins` directory.

> *NOTE*: You will also need to
> [configure access](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/deployment/filesystems/s3/#configure-access-credentials)
> from your job to AWS S3. That is outside the scope of this readme.

### Avro Support

Flinkrunner supports reading and writing avro messages with kafka and file systems. For
kafka sources and sinks, Flinkrunner uses binary encoding with Confluent schema registry.
For file sources and sinks, you can select either standard or parquet avro encoding.

Add the following dependencies if you need Avro and Confluent schema registry support:

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

Flinkrunner provides automatic support for serializing and deserializing in and out of
kafka using
Confluent's [Avro schema registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
. To make use of this support, you need to do three things:

1. Include the `kafka-avro-serializer` library in your project (as described above).
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
3. Mixin the `EmbeddedAvroRecord` and `EmbeddedAvroRecordFactory` traits for your event
   types that need to decode/encode Confluent schema registry compatible avro records in
   Kafka.
4. If your output event type is an avro record, use the `AvroStreamJob` base class for
   your flink jobs instead of `StreamJob`. Note, that both `AvroStreamJob` and
   `StreamJob` can read from avro source streams, but `AvroStreamJob` uses an avro sink.
   This is described in more detail below.

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
libraryDependencies +=   "ch.qos.logback" % "logback-classic" % "1.2.11"
```

### Complex Event Processing

If you want to use the complex event processing library, add this dependency:

```
"org.apache.flink" % "flink-cep" % <flink-version>
```

## What is FlinkRunner?

`FlinkRunner` helps you think about your flink jobs at a high level, so you can focus on
the event pipeline, not the plumbing.

You have a set of related flink jobs that deal in a related set of data event
types. `Flinkrunner` helps you build one application to run those related jobs and
coordinate the types. It also simplifies setting up common sources and sinks, so you can
control them purely with configuration, not code. `Flinkrunner` supports a variety of
sources and sinks out of the box, including `kafka`, `kinesis`, `jdbc`
, `elasticsearch 7+` (sink only), `cassandra` (sink only),
`filesystems` (including `s3` using `parquet` as well as `delimited` and `json` text
files) and`sockets`. It also has many common operators to help you in writing your own
transformation logic. Finally, `FlinkRunner`
makes it easy to test your transformation logic with property-based testing.

## Get Started

First, you need to define an [*algebraic data
type*](http://tpolecat.github.io/presentations/algebraic_types.html#1) (ADT) containing
the types that will be used in your flink jobs. Your top level type should inherit
from `FlinkEvent`. This requires your types to implement the following members:

* `$id: String` - a unique id for the event
* `$timestamp: Long` - the event time (millis since unix epoch)
* `$key: String` - a key to group events

Additionally, a `FlinkEvent` has three additional members that you can optionally override
to enable/configure various features.

* `$bucketId: String` - for bucketing files written to a file sink
* `$dedupId: String` - for use with Flinkrunner's deduplication filter
* `$active:Boolean` - for use with Flinkrunner's filterByControl source stream algorithm

```
sealed trait MyEventADT extends FlinkEvent
final case class MyEventA(a:Int, $id:String, $key:String, $timestamp:Long)
final case class MyEventB(b:Int, $id:String, $key:String, $timestamp:Long)
```

Next, you should create your own runner subclass of the abstract `FlinkRunner` base class.

```
class MyRunner(config:FlinkConfig) extends FlinkRunner[MyEventADT](config) {

  override def invoke(jobName:String):Unit = jobName matches {
    case "MyJob1" => new MyJob1(this).run()
    case "MyJob2" => new MyJob2(this).run()
    case _        => throw new RuntimeException(s"Unknown job $jobName")
  }
}
```

Next, write some jobs! This is the fun part.

```
class MyJob1(runner:FlinkRunner[MyEventADT]) extends StreamJob[MyEventA](runner) {

  override def transform:DataStream[MyEventA] =
    singleSource[MyEventA]()

}
```

This job takes `MyEventA` type input events and passes them through unchanged to the sink.
While this sounds useless at first blush, and your `transform` method will usually do much
more interesting things, identity transforms like this can be useful to copy data from one
storage system to another.

Next, you need to configure your job. The following configuration defines a file source
that reads csv files from one s3 bucket and a file sink that writes json files to a
different s3 bucket.

```hocon
jobs {
  MyJob1 {
    sources {
      csv-file-source {
        path = "s3://my-event-a-csv-files"
        format = csv
      }
    }
    sinks {
      json-file-sink {
        path = "s3://my-event-a-json-files"
        format = json
      }
    }
  }
}
```

Next, wire up your runner to a main method.

  ```
  object Main {
    def main(args:Array[String]) =
      new MyRunner(new FlinkConfig(args))
  }
  ```

Finally, assemble an uber jar with all your dependencies, deploy it to your flink cluster,
and run a job:

```
flink run myRunner.jar MyJob1
```

## Flink Jobs

`Flinkrunner` provides a `StreamJob` base class from which you can build and run your
flink jobs. If your output event types require avro support, you should instead use the
`AvroStreamJob` base class to build and run your flink job.

### StreamJob

```
class StreamJob[
  OUT <: ADT,
  ADT <: FlinkEvent](runner:FlinkRunner[ADT])
```

A `StreamJob` must specify the output stream event type in its definition. That output
stream event type must be a subclass of your `Flinkrunner` algebraic data type (ADT). Your
job class will be passed an instance of your `Flinkrunner` type, from which you can access

* `runner.config`: `Flinkrunner` configuration (instance of `FlinkConfig`).
* `runner.env`: Flink's stream execution environment (instance of
  `StreamExecutionEnvironment`), for interfacing with the DataStream API.
* `runner.tableEnv`: Flink's stream table environment (instance of
  `StreamTableEnvironment`), for interfacing with the Table API.
* `runner.mockEdges`: A boolean indicating if your runner instance will mock interaction
  with sources and sinks and redirect transformed output to your
  specified `CheckResults.checkOutputEvents` method.

Your `StreamJob` must provide a `transform` method that defines and transforms your
sources into an output event stream. `StreamJob` provides several factory methods to
create source streams from your configuration:

* `singleSource[IN <: ADT](name:String)`: produces a single input source stream,
  configured under the provided name, of type `DataStream[IN]`, where `IN` is an event
  type within your runner's `ADT`. The configured `name` parameter defaults to the first
  configured source for convenience.

* `connectedSource[IN1 <: ADT, IN2 <: ADT, KEY](name1:String, name2:String, key1:IN1=>KEY, key2:IN2=>KEY): ConnectedStreams[IN1,IN2]`:
  connects the two streams, configured under the provided names, producing a single stream
  of type `ConnectedStreams[IN1,IN2]`. A connected stream is effectively a union of the
  two types of stream. An event on the connected stream is either of type `IN1` or `IN2`.
  The key functions are used for any `keyBy` operations done on the connected stream. It
  is important to realize a connected stream is NOT a join between the two streams, and
  the keys are not used to perform a join. The use case for a connected stream is where
  the data on one stream dynamically impacts how you want to process the other stream.

* `filterByControlSource[CONTROL <: ADT, DATA <: ADT, KEY](controlName:String, dataName:String, key1:CONTROL=>KEY, key2:DATA=>KEY): DataStream[DATA]`:
  is a special instance of connected sources, where the first source is a control stream
  and the second source is a data stream. The control events indicate when the data event
  stream should be considered active, meaning any data events seen should be emitted. When
  the control type's `$active`
  method returns true, the data stream will be considered active, and any data events seen
  on the connected stream will be emitted to the output. Conversely, when the control
  type's `$active` method returns
  `false`, the data stream will be considered inactive, and any data events seen on the
  connected stream will not be emitted to the output.

* `broadcastConnectedSource[
  IN <: ADT: TypeInformation, BC <: ADT: TypeInformation, KEY: TypeInformation](
  keyedSourceName: String, broadcastSourceName: String, keyedSourceGetKeyFunc: IN => KEY)
  : BroadcastConnectedStream[IN, BC]`: another special connected source that implements
  Flink's [Broadcast State Pattern](https://nightlies.apache.
  org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/broadcast_state/).
  This `StreamJob` method keys and connects a regular data stream, with a so-called
  broadcast data stream. A broadcast stream sends copies of all of its elements to all
  downstream tasks. So in this case, we key the source data function and effectively send
  a connected stream of the data and broadcast elements to each keyed task. A common use
  case broadcasts a stream of rule changes that impact how the data stream should be
  processed. The `BroadcastConnectedStream[IN,BC]` should be processed with a special type
  of `CoProcessFunction` called a `KeyedBroadcastProcessFunction[KEY, IN, BC, OUT]`, which
  produces your transformed output data stream of type `DataStream[OUT]`.

`StreamJob` also provides avro versions of all these source factory methods. If your
sources are Confluent schema-registry aware avro encoded Kafka streams, you should use the
avro aware versions of these factory methods. For instance, the `singleAvroSource()`
method can be used to produce such an input datatream. The signatures of these methods are
more complicated and rely on you to use Flinkrunner's `EmbeddedAvroRecord` and
`EmbeddedAvroRecordFactory` traits when implementing your event types.

```
singleAvroSource[
      IN <: ADT with EmbeddedAvroRecord[INA],
      INA <: GenericRecord](
      name: String)(implicit
      fromKV: EmbeddedAvroRecordInfo[INA] => IN,
      avroParquetRecordFormat: StreamFormat[INA]): DataStream[IN]
```

Besides source factory methods, `StreamJob` also provides a method to easily perform
[windowed aggregations](https://nightlies.apache.
org/flink/flink-docs-master/docs/dev/table/sql/queries/window-agg/).

```
def windowedAggregation[
      E <: ADT: TypeInformation,
      KEY: TypeInformation,
      WINDOW <: Window: TypeInformation,
      AGG <: Aggregate: TypeInformation,
      QUANTITY <: Quantity[QUANTITY]: TypeInformation,
      PWF_OUT <: ADT: TypeInformation](
      source: KeyedStream[E, KEY],
      initializer: WindowedAggregationInitializer[
        E,
        KEY,
        WINDOW,
        AGG,
        QUANTITY,
        PWF_OUT,
        ADT
      ]): DataStream[PWF_OUT]
```

Finally, `StreamJob` also provides a `run()` method that builds and executes the flink job
graph defined by your `transform` method.

### AvroStreamJob

```
class AvroStreamJob[
  OUT <: ADT with EmbeddedAvroRecord[A],
  A<:GenericRecord,
  ADT <: FlinkEvent](runner:FlinkRunner[ADT])
```

An `AvroStreamJob` is a specialized `StreamJob` class to support outputting to an avro
encoded sink (kafka or parquet-avro files).

#### EmbeddedAvroRecord

```
trait EmbeddedAvroRecord[A <: GenericRecord {

  def $recordKey: Option[String] = None

  def $record: A

  def $recordHeaders: Map[String, String] = Map.empty

  def toKV: EmbeddedAvroRecordInfo[A] =
    EmbeddedAvroRecordInfo($record, $recordKey, $recordHeaders)
}
```

#### EmbeddedAvroRecordFactory

```
trait EmbeddedAvroRecordFactory[
    E <: FlinkEvent with EmbeddedAvroRecord[A],
    A <: GenericRecord] {

  implicit def fromKV(recordInfo: EmbeddedAvroRecordInfo[A]): E

  implicit def avroParquetRecordFormat: StreamFormat[A] = ???
}
```

#### EmbeddedAvroRecordInfo

```
case class EmbeddedAvroRecordInfo[A <: GenericRecord](
    record: A,
    keyOpt: Option[String] = None,
    headers: Map[String, String] = Map.empty)
```

#### EmbeddedAvroParquetRecordFactory

An avro parquet writer factory for types that wrap an avro record.

#### EmbeddedAvroParquetRecordFormat

A StreamFormat to read parquet avro files containing a type that embeds an avro record.

## Flinkrunner Configuration

`Flinkrunner` uses [lightbend config](https://lightbend.github.io/config/) for its
configuration, integrating environment variables and command line arguments to provide
easy, environment specific, 12-factor style configuration. `Flinkrunner` makes this
configuration accessible through a `FlinkConfig` object that you construct and pass to
your runner.

### SourceConfigs

```
trait SourceConfig[ADT <: FlinkEvent]
```

#### File Source

#### Kafka Source

#### Hybrid Source

#### Kinesis Source

#### RabbitMQ Source

#### Socket Source

### SinkConfigs

```
trait SinkConfig[ADT <: FlinkEvent]
```

#### Cassandra Sink

#### Elasticsearch Sink

#### File Sink

#### Jdbc Sink

#### Kafka Sink

#### Kinesis Sink

#### RabbitMQ Sink

#### Socket Sink

## Aggregations

```
trait Aggregate {

    def name:String

    // the kind of measurement being aggregated
    def dimension: String

    // the preferred unit of measurements being aggregated
    def unit: String

    // the current aggregated value
    def value: Double

    // the current count of measurements included in this aggregate
    def count: BigInt

    // the timestamp of the most recent aggregated event
    def aggregatedLastUpdated: Instant

    // the timestamp when this aggregate was last updated
    def lastUpdated: Instant

    // other aggregations this aggregation depends on
    def dependentAggregations: Map[String, Aggregate]

    // configuration parameters
    def params: Map[String, String]

    // a method to update the aggregate with a new value
    def update(value: Double, unit: String,
               aggLU: Instant): Try[Aggregate]
}
```

### Quantities and Units

### Built-in Aggregates

#### Count

#### Exponential Moving Average

#### Exponential Moving Standard Deviation

#### Exponential Moving Variance

#### Histogram

#### Max

#### Mean

#### Min

#### Percentage

#### Range

#### Standard Deviation

#### Sum

#### Sum of Squared Deviations

#### Variance

## Operators

### SBFDeduplicationFilter

### EnrichmentAsyncFunction

### FlinkRunnerAggregateFunction

## Algorithms

### Cardinality

#### HyperLogLog

### Set Membership

#### Stable Bloom Filter

## Serialization/Deserialization

### Deserialization

#### ConfluentAvroRegistryKafkaRecordDeserializationSchema

#### DelimitedRowDecoder

#### JsonDeserializationSchema

#### JsonKafkaRecordDeserializationSchema

#### JsonKinesisDeserializationSchema

#### JsonRMQDeserializationSchema

#### JsonRowDecoder

### Serialization

#### ConfluentAvroRegistryKafkaRecordSerializationSchema

#### DelimitedFileEncoder

#### DelimitedRowEncoder

#### EmbeddedAvroDelimitedFileEncoder

#### EmbeddedAvroJsonFileEncoder

#### EmbeddedAvroJsonKafkaRecordSerializationSchema

#### EmbeddedAvroJsonKinesisSerializationSchema

#### EmbeddedAvroJsonSerializationSchema

#### JsonFileEncoder

#### JsonKafkaRecordSerializationSchema

#### JsonKinesisSerializationSchema

#### JsonRowEncoder

#### JsonSerializationSchema

## Testing

### CheckResults

### Property Testing

## Utilities

Some useful utilities.

#### AWS Signer

#### Bounded Lateness Support

#### Sql Support
