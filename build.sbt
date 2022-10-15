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
  val flink               = "1.15.2"
  val logback             = "1.2.11"
  val scalaLogging        = "3.9.5"
  val scalaTest           = "3.2.14"
  val scalaTestPlus       = "3.2.14.0"
  val scalaCheck          = "1.16.0"
  val testContainersScala = "0.40.10"
  val jackson             = "2.13.4"
  val circe               = "0.14.2"
  val http4s              = "0.23.12"
  val enumeratum          = "1.7.0"
  val typesafeConfig      = "1.4.2"
  val guava               = "31.1-jre"
  val squants             = "1.8.3"
  val confluentAvroSerde  = "7.1.1"
  val parquet             = "1.12.3"
  val awsSdk              = "1.12.315"
  val jdbcMysql           = "8.0.30"
  val jdbcPg              = "42.5.0"
  val jdbcMssql           = "11.2.0.jre11"
  val hadoop              = "3.3.2"
  val jna                 = "5.12.1" // needed for testcontainers in some jvms
}

val flinkDeps =
  Seq(
    // scala
    "org.apache.flink" %% "flink-scala"                    % V.flink,
    "org.apache.flink" %% "flink-streaming-scala"          % V.flink,
    // rocksdb
    "org.apache.flink"  % "flink-statebackend-rocksdb"     % V.flink,
    // queryable state
    "org.apache.flink"  % "flink-queryable-state-runtime"  % V.flink % Provided,
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
    "org.apache.flink" %% "flink-table-api-scala-bridge"   % V.flink, // table api scala
    "org.apache.flink"  % "flink-table-planner-loader"     % V.flink % Provided, // table api
    "org.apache.flink"  % "flink-table-runtime"            % V.flink % Provided, // table runtime
    "org.apache.flink"  % "flink-csv"                      % V.flink % Provided, // table api csv format
    "org.apache.flink"  % "flink-json"                     % V.flink % Provided, // table api json format
    "org.apache.flink"  % "flink-clients"                  % V.flink,
    // test support
    "org.apache.flink"  % "flink-test-utils"               % V.flink % Test,
    "org.apache.flink"  % "flink-runtime-web"              % V.flink % Test
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
  "org.apache.hadoop"                % "hadoop-client"                    % V.hadoop              % Provided,
  "io.confluent"                     % "kafka-avro-serializer"            % V.confluentAvroSerde  % Provided,
  "com.amazonaws"                    % "aws-java-sdk-core"                % V.awsSdk              % Provided,
  "com.beachape"                    %% "enumeratum"                       % V.enumeratum,
  "com.typesafe"                     % "config"                           % V.typesafeConfig,
  "com.google.guava"                 % "guava"                            % V.guava,
  "org.typelevel"                   %% "squants"                          % V.squants,
  "org.scalactic"                   %% "scalactic"                        % V.scalaTest,
  "org.scalatest"                   %% "scalatest"                        % V.scalaTest           % Test,
  "org.scalatestplus"               %% "scalacheck-1-17"                  % V.scalaTestPlus       % Test,
  "org.scalacheck"                  %% "scalacheck"                       % V.scalaCheck,
  "com.fasterxml.jackson.module"    %% "jackson-module-scala"             % V.jackson,
  "com.github.pjfanning"            %% "jackson-scala-reflect-extensions" % V.jackson,
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-csv"           % V.jackson,
  "com.fasterxml.jackson.datatype"   % "jackson-datatype-jsr310"          % V.jackson,
  "com.dimafeng"                    %% "testcontainers-scala-scalatest"   % V.testContainersScala % Test,
  "com.dimafeng"                    %% "testcontainers-scala-mysql"       % V.testContainersScala % Test,
  "mysql"                            % "mysql-connector-java"             % V.jdbcMysql           % Provided,
  "com.dimafeng"                    %% "testcontainers-scala-postgresql"  % V.testContainersScala % Test,
  "org.postgresql"                   % "postgresql"                       % V.jdbcPg              % Provided,
  "com.dimafeng"                    %% "testcontainers-scala-mssqlserver" % V.testContainersScala % Test,
  "net.java.dev.jna"                 % "jna"                              % V.jna                 % Test,
  "com.microsoft.sqlserver"          % "mssql-jdbc"                       % V.jdbcMssql           % Provided
) ++
  Seq("org.apache.parquet" % "parquet-avro" % V.parquet % Provided).map(
    m =>
      m.excludeAll(
        ExclusionRule(
          organization = "org.apache.hadoop",
          name = "hadoop-client"
        ),
        ExclusionRule(
          organization = "it.unimi.dsi",
          name = "fastutil"
        )
      )
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

lazy val AvroGenSettings = Seq(
  Test / sourceGenerators += (Test / avroScalaGenerateSpecific).taskValue,
  Test / avroSpecificSourceDirectories += (Test / resourceDirectory).value / "avro",
  Test / avroSpecificScalaSource := {
    val base = thisProject.value.base
    new File(
      new File(
        new File(
          new File(new File(base, "target"), "scala-2.12"),
          "src_managed"
        ),
        "test"
      ),
      "compiled_avro"
    )
  },
  Test / avroScalaCustomTypes    :=
    avrohugger.format.Standard.defaultTypes
      .copy(timestampMillis = JavaTimeInstant),
  Test / managedSourceDirectories ++= baseDirectory { base =>
    Seq(
      base / "target/scala/src_managed/test/compiled_avro"
    )
  }.value
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
  .settings(AvroGenSettings: _*)

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
Test / fork          := true

Compile / run := Defaults
  .runTask(
    Compile / fullClasspath,
    Compile / run / mainClass,
    Compile / run / runner
  )
  .evaluated
