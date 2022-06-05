import avrohugger.types.JavaTimeInstant

name := "flinkrunner"

lazy val scala212               = "2.12.15"
lazy val supportedScalaVersions = List(scala212)

inThisBuild(
  List(
    organization := "io.epiphanous",
    homepage     := Some(url("https://github.com/epiphanous/flinkrunner")),
    licenses     := List("MIT" -> url("https://opensource.org/licenses/MIT")),
    developers   := List(
      Developer(
        "nextdude",
        "Robert Lyons",
        "nextdude@gmail.com",
        url("https://epiphanous.io")
      )
    ),
    scalaVersion := scala212
  )
)

Test / envVars           := Map("TEST_ENV_EXISTS" -> "exists")
Test / parallelExecution := false
Test / fork              := true
resolvers += "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository"
resolvers += "Confluent Repository" at "https://packages.confluent.io/maven/"

val V = new {
  val flink        = "1.15.0"
  val logback      = "1.2.11"
  val scalaLogging = "3.9.4"
  val scalaTest    = "3.2.12"
  val scalaCheck   =
    "3.2.10.0" // 3.2.11.0 throws a conflict on scala-xml with scala-compiler
  val circe              = "0.14.2"
  val http4s             = "0.23.12"
  val enumeratum         = "1.7.0"
  val typesafeConfig     = "1.4.2"
  val guava              = "31.1-jre"
  val squants            = "1.8.3"
  val confluentAvroSerde = "7.1.1"
  val parquet            = "1.12.3"
  val awsSdk             = "1.12.233"
}

val flinkDeps =
  Seq(
    "org.apache.flink" %% "flink-scala"                    % V.flink, // scala
    "org.apache.flink" %% "flink-streaming-scala"          % V.flink, // ds api scala
    "org.apache.flink"  % "flink-statebackend-rocksdb"     % V.flink,
    // complex event processing
    "org.apache.flink"  % "flink-cep"                      % V.flink % Provided,
    // connectors
    "org.apache.flink"  % "flink-connector-base"           % V.flink % Provided, // ds hybrid source
    "org.apache.flink"  % "flink-connector-files"          % V.flink % Provided, // ds text files
    "org.apache.flink"  % "flink-parquet"                  % V.flink % Provided, // parquet bulk sink
    "org.apache.flink"  % "flink-connector-kafka"          % V.flink % Provided,
    "org.apache.flink"  % "flink-connector-kinesis"        % V.flink % Provided,
    "org.apache.flink" %% "flink-connector-cassandra"      % V.flink % Provided,
    "org.apache.flink"  % "flink-connector-elasticsearch7" % V.flink % Provided,
    "org.apache.flink"  % "flink-connector-jdbc"           % V.flink % Provided,
    "org.apache.flink"  % "flink-connector-rabbitmq"       % V.flink % Provided,
    // avro support
    "org.apache.flink"  % "flink-avro"                     % V.flink % Provided, // ds and table avro format
    "org.apache.flink"  % "flink-avro-confluent-registry"  % V.flink % Provided, // ds and table avro registry format
    // table api support
    "org.apache.flink" %% "flink-table-api-scala-bridge"   % V.flink % Provided, // table api scala
    "org.apache.flink" %% "flink-table-planner"            % V.flink % Provided, // table api
    "org.apache.flink"  % "flink-csv"                      % V.flink % Provided, // table api csv format
    "org.apache.flink"  % "flink-json"                     % V.flink % Provided, // table api json format
    // test support
    "org.apache.flink"  % "flink-test-utils"               % V.flink % Test
  )

val loggingDeps = Seq(
  "ch.qos.logback"              % "logback-classic" % V.logback % Provided,
  "com.typesafe.scala-logging" %% "scala-logging"   % V.scalaLogging
)

val http4sDeps = Seq(
  "dsl",
  "ember-client",
  "circe"
).map(d => "org.http4s" %% s"http4s-$d" % V.http4s)

val circeDeps = Seq(
  "core",
  "generic",
  "generic-extras",
  "parser"
).map(d => "io.circe" %% s"circe-$d" % V.circe)

val otherDeps = Seq(
  "io.confluent"       % "kafka-avro-serializer" % V.confluentAvroSerde % Provided,
  "org.apache.parquet" % "parquet-avro"          % V.parquet            % Provided,
  "com.amazonaws"      % "aws-java-sdk-core"     % V.awsSdk             % Provided,
  "com.beachape"      %% "enumeratum"            % V.enumeratum,
  "com.typesafe"       % "config"                % V.typesafeConfig,
  "com.google.guava"   % "guava"                 % V.guava,
  "org.typelevel"     %% "squants"               % V.squants,
  "org.scalatestplus" %% "scalacheck-1-15"       % V.scalaCheck,
  "org.scalactic"     %% "scalactic"             % V.scalaTest          % Test,
  "org.scalatest"     %% "scalatest"             % V.scalaTest          % Test
)

/** Exclude any transitive deps using log4j
  * @param m
  *   the module
  * @return
  *   module with deps excluded
  */
def excludeLog4j(m: ModuleID) = m.excludeAll(
  ExclusionRule(
    organization = "org.apache.logging.log4j",
    name = "*"
  ),
  ExclusionRule(organization = "org.slf4j", name = "*")
)

lazy val flink_runner = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildInfoKeys      := Seq[BuildInfoKey](
      name,
      version,
      scalaVersion,
      sbtVersion
    ),
    buildInfoPackage   := "io.epiphanous.flinkrunner",
    buildInfoOptions += BuildInfoOption.ToMap,
    buildInfoOptions += BuildInfoOption.ToJson,
    buildInfoOptions += BuildInfoOption.BuildTime,
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies ++= (flinkDeps ++ http4sDeps ++ circeDeps ++ otherDeps)
      .map(excludeLog4j) ++ loggingDeps
  )

Test / sourceGenerators += (Compile / avroScalaGenerateSpecific).taskValue
Test / avroScalaCustomTypes :=
  avrohugger.format.Standard.defaultTypes
    .copy(timestampMillis = JavaTimeInstant)

scalacOptions ++= Seq(
  "-encoding",
  "utf8",
  "-deprecation",
  "-Xfuture",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-unused",
  "-Ywarn-value-discard"
)

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable  := true

Compile / run := Defaults
  .runTask(
    Compile / fullClasspath,
    Compile / run / mainClass,
    Compile / run / runner
  )
  .evaluated
