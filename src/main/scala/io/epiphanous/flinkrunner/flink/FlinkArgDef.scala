package io.epiphanous.flinkrunner.flink

case class FlinkArgDef(name: String, text: String, default: Option[String] = None)

//Boolean("checkpoint.flash")
//Boolean("checkpoint.incremental")
//Int("checkpoint.max.concurrent")
//Int("global.parallelism")
//Int("port")
//Long("bucket.check.interval")
//Long("bucket.rolling.policy.inactivity.interval")
//Long("bucket.rolling.policy.max.part.size")
//Long("bucket.rolling.policy.rollover.interval")
//Long("checkpoint.interval")
//Long("checkpoint.min.pause")
//Long("control.lockout.duration")
//Long("max.active.duration")
//Long("max.lateness")
//String("bucket.assigner")
//String("bucket.assigner.datetime.format")
//String("bucket.rolling.policy")
//String("checkpoint.url")
//String("encoder.format")
//String("host")
//String("path")
//String("state.backend")
//String("time.characteristic")
//String("topic")

object FlinkArgDef {
  val CORE_FLINK_ARGUMENTS = Set(
    FlinkArgDef("environment", "specify running environment (prod, stage, dev)", default = Some("dev")),
    FlinkArgDef("mock.edges", "Mock sources and sinks for testing", default = Some("true")),
    FlinkArgDef("debug", "Print debug messages to stdout", default = Some("true")),
    FlinkArgDef("show.plan",
                "True if you want to emit an INFO log statement to show the execution plan",
                default = Some("false")),
    FlinkArgDef("global.parallelism", "number of tasks slots allocated to job", default = Some("1")),
    FlinkArgDef("checkpoint.interval", "how often to start a new checkpoint (milliseconds)", default = Some("30000")),
    FlinkArgDef("checkpoint.min.pause", "min duration between checkpoints (milliseconds)", default = Some("15000")),
    FlinkArgDef("checkpoint.max.concurrent", "max number of concurrent checkpoints", default = Some("1")),
    FlinkArgDef("checkpoint.url", "where to store checkpoints", default = Some("file:///tmp/checkpoint")),
    FlinkArgDef("checkpoint.flash", "whether to optimize for flash storage", default = Some("true")),
    FlinkArgDef("kafka.sources", "comma-separated list of prefixes for required kafka sources", default = Some("")),
    FlinkArgDef("kafka.source.bootstrap.servers", "kafka broker(s) for source", default = Some("localhost:9092")),
    FlinkArgDef("kafka.source.group.id", "kafka source consumer group id", default = Some("foobar")),
    FlinkArgDef("kafka.source.auto.offset.reset", "kafka source offset to start at", default = Some("latest")),
    FlinkArgDef("kafka.source.topic", "kafka source topic name", default = None),
    FlinkArgDef("kafka.sinks", "comma-separated list of prefixes for required kafka sinks", default = Some("")),
    FlinkArgDef("kafka.sink.bootstrap.servers", "kafka broker(s) for sink"),
    FlinkArgDef("kafka.sink.topic", "kafka sink topic name"),
    FlinkArgDef("kinesis.sources", "comma-separated list of prefixes for required kafka sources", default = Some("")),
    FlinkArgDef("kinesis.source.aws.region", "kinesis source aws region", default = Some("us-east-1")),
    FlinkArgDef("kinesis.source.aws.credentials.provider",
                "aws credentials provider for source",
                default = Some("AUTO")),
    FlinkArgDef("kinesis.source.flink.stream.initpos", "kinesis source starting position", default = Some("LATEST")),
    FlinkArgDef("kinesis.source.topic", "kinesis source topic name", default = None),
    FlinkArgDef("kinesis.sinks", "comma-separated list of prefixes for required kafka sinks", default = Some("")),
    FlinkArgDef("kinesis.sink.aws.region", "kinesis sink aws region", default = Some("us-east-1")),
    FlinkArgDef("kinesis.sink.aws.credentials.provider", "aws credentials provider for sink", default = Some("AUTO")),
    FlinkArgDef("kinesis.sink.topic", "kinesis sink topic name"),
    FlinkArgDef("event.store", "the event store (\"kafka\" or \"kinesis\")", Some("kafka")),
    FlinkArgDef("jdbc.sinks", "comma-separated list of prefixes for required jdbc sinks", default = Some("")),
    FlinkArgDef("jdbc.sink.driver.name", "class name of the jdbc driver", default = Some("org.postgresql.Driver")),
    FlinkArgDef("jdbc.sink.url",
                "jdbc url for the database connection",
                default = Some("jdbc:postgresql://localhost/")),
    FlinkArgDef("jdbc.sink.username", "username for the database connection", default = None),
    FlinkArgDef("jdbc.sink.password", "password for the database connection", default = None),
    FlinkArgDef("jdbc.sink.query", "query for the prepared insertion of records", default = None),
    FlinkArgDef("jdbc.sink.buffer.size",
                "number of records to buffer before flushing to database",
                default = Some("32")),
    FlinkArgDef("cassandra.sinks", "comma-separated list of prefixes for required cassandra sinks"),
    FlinkArgDef("cassandra.sink.host", "host name for the cassandra cluster", default = Some("localhost")),
    FlinkArgDef("cassandra.sink.port", "port for the cassandra cluster", default = Some("9042")),
    FlinkArgDef("cassandra.sink.query", "query for the cassandra sink", default = None),
    FlinkArgDef("max.lateness", "max number of milliseconds current events may arrive late", default = Some("5000")),
    FlinkArgDef("control.lockout.duration",
                "how long to wait after control stream active to emit events (in seconds)",
                default = Some("0")),
    FlinkArgDef("max.active.duration", "max length of an active control period (in seconds)", default = Some("57600")),
    FlinkArgDef("max.control.lateness", "max number of seconds control events may arrive late", default = Some("5")),
    FlinkArgDef("state.backend", "type of backend for checkpointed state", default = Some("rocksdb"))
  )
}
