import avrohugger.types.JavaTimeInstant

name := "flinkrunner"

lazy val scala2                 = "2.12.18"
lazy val supportedScalaVersions = List(scala2)

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
    scalaVersion := scala2
  )
)

Test / envVars           := Map("TEST_ENV_EXISTS" -> "exists")
Test / parallelExecution := false
Test / fork              := true
resolvers += "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository"
resolvers += "Confluent Repository" at "https://packages.confluent.io/maven/"

val V = new {
  val flink                  = "1.19.1"
  val flinkMinor             = s"${flink.replaceFirst("\\.[0-9]+$", "")}"
  val fcKinesis              = s"4.3.0-$flinkMinor"
  val fcKafka                = s"3.2.0-$flinkMinor"
  val fcCassandra            = s"3.2.0-$flinkMinor"
  val fcJdbc                 = s"3.1.0-1.17"
  val fcRabbitMq             = s"3.0.1-1.17"
  val fcElastic              = s"3.0.1-1.17"
  // -------------------------------------------------
  val awsSdk                 = "1.12.506"
  val awsSdk2                = "2.20.103"
  val cassandraDriver        = "3.11.3"
  val circe                  = "0.14.5"
  val circeGenericExtras     = "0.14.3"
  val confluentAvroSerde     = "7.4.1"
  val dropWizard             = "4.2.19"
  val enumeratum             = "1.7.2"
  val glueSerde              = "1.1.15"
  val guava                  = "32.1.1-jre"
  val hadoop                 = "3.3.6"
  val http4s                 = "0.23.22"
  val iceberg                = "1.6.1"
  val jackson                = "2.15.2"
  val jacksonScalaReflectExt = "2.15.0"
  val jdbcMssql              = "11.2.0.jre11"
  val jdbcMysql              = "8.0.33"
  val jdbcPg                 = "42.6.0"
  val jna                    = "5.13.0" // needed for testcontainers in some JVM
  val logback                = "1.4.8"
  val parquet                = "1.13.1"
  val requests               = "0.8.0"
  val scalaCheck             = "1.17.0"
  val scalaLogging           = "3.9.5"
  val scalaTest              = "3.2.16"
  val scalaTestPlus          = "3.2.16.0"
  val squants                = "1.8.3"
  val testContainersScala    = "0.40.17"
  val typesafeConfig         = "1.4.2"
  val uuidCreator            = "5.3.2"
  val mockito                = "1.17.14"
}

val flinkDeps =
  Seq(
    // scala
    "org.apache.flink" %% "flink-scala"                          % V.flink,
    "org.apache.flink" %% "flink-streaming-scala"                % V.flink,
    // rocksdb
    "org.apache.flink"  % "flink-statebackend-rocksdb"           % V.flink,
    // sql parser
    "org.apache.flink"  % "flink-sql-parser"                     % V.flink,
    // queryable state
    "org.apache.flink"  % "flink-queryable-state-runtime"        % V.flink       % Provided,
    // complex event processing
    "org.apache.flink"  % "flink-cep"                            % V.flink       % Provided,
    // connectors
    "org.apache.flink"  % "flink-connector-base"                 % V.flink       % Provided, // ds hybrid source
    "org.apache.flink"  % "flink-connector-files"                % V.flink       % Provided, // ds text files
    "org.apache.flink"  % "flink-parquet"                        % V.flink       % Provided, // parquet bulk sink
    "org.apache.flink"  % "flink-connector-kafka"                % V.fcKafka     % Provided,
    "org.apache.flink"  % "flink-connector-kinesis"              % V.fcKinesis   % Provided,
    "org.apache.flink"  % "flink-connector-aws-kinesis-streams"  % V.fcKinesis   % Provided,
    "org.apache.flink"  % "flink-connector-aws-kinesis-firehose" % V.fcKinesis   % Provided,
    "org.apache.flink" %% "flink-connector-cassandra"            % V.fcCassandra % Provided,
    "org.apache.flink"  % "flink-connector-elasticsearch7"       % V.fcElastic   % Provided,
    "org.apache.flink"  % "flink-connector-jdbc"                 % V.fcJdbc      % Provided,
    "org.apache.flink"  % "flink-connector-rabbitmq"             % V.fcRabbitMq  % Provided,
    "org.apache.flink"  % "flink-connector-datagen"              % V.flink       % Provided,
    // avro support
    "org.apache.flink"  % "flink-avro"                           % V.flink       % Provided, // ds and table avro format
    "org.apache.flink"  % "flink-avro-confluent-registry"        % V.flink       % Provided, // ds and table avro registry format
    // table api support
    "org.apache.flink" %% "flink-table-api-scala-bridge"         % V.flink, // table api scala
    "org.apache.flink"  % "flink-table-planner-loader"           % V.flink       % Provided, // table api
    "org.apache.flink"  % "flink-table-runtime"                  % V.flink       % Provided, // table runtime
    "org.apache.flink"  % "flink-csv"                            % V.flink       % Provided, // table api csv format
    "org.apache.flink"  % "flink-json"                           % V.flink       % Provided, // table api json format
    "org.apache.flink"  % "flink-clients"                        % V.flink,
    // dropwizard metrics support
    "org.apache.flink"  % "flink-metrics-dropwizard"             % V.flink       % Provided,
    // test support
    "org.apache.flink"  % "flink-test-utils"                     % V.flink,
    "org.apache.flink"  % "flink-runtime-web"                    % V.flink       % Test
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
  "parser"
).map(d => "io.circe" %% s"circe-$d" % V.circe)

val otherDeps = Seq(
  "io.circe"                        %% "circe-generic-extras"                   % V.circeGenericExtras,
  "com.amazonaws"                    % "aws-java-sdk-core"                      % V.awsSdk              % Provided,
  "com.amazonaws"                    % "aws-java-sdk-s3"                        % V.awsSdk              % Test,
  "software.amazon.glue"             % "schema-registry-flink-serde"            % V.glueSerde,
  "com.beachape"                    %% "enumeratum"                             % V.enumeratum,
  "com.datastax.cassandra"           % "cassandra-driver-extras"                % V.cassandraDriver     % Provided,
  "com.dimafeng"                    %% "testcontainers-scala-cassandra"         % V.testContainersScala % Test,
  "com.dimafeng"                    %% "testcontainers-scala-localstack-v2"     % V.testContainersScala % Test,
  "com.dimafeng"                    %% "testcontainers-scala-mssqlserver"       % V.testContainersScala % Test,
  "com.dimafeng"                    %% "testcontainers-scala-mysql"             % V.testContainersScala % Test,
  "com.dimafeng"                    %% "testcontainers-scala-postgresql"        % V.testContainersScala % Test,
  "com.dimafeng"                    %% "testcontainers-scala-scalatest"         % V.testContainersScala % Test,
  "com.dimafeng"                    %% "testcontainers-scala-kafka"             % V.testContainersScala % Test,
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-csv"                 % V.jackson,
  "com.fasterxml.jackson.datatype"   % "jackson-datatype-jsr310"                % V.jackson,
  "com.fasterxml.jackson.module"    %% "jackson-module-scala"                   % V.jackson,
  "com.github.f4b6a3"                % "uuid-creator"                           % V.uuidCreator,
  "com.github.pjfanning"            %% "jackson-scala-reflect-extensions"       % V.jacksonScalaReflectExt,
  "com.google.guava"                 % "guava"                                  % V.guava,
  "com.lihaoyi"                     %% "requests"                               % V.requests            % Test,
  "com.microsoft.sqlserver"          % "mssql-jdbc"                             % V.jdbcMssql           % Provided,
  "com.typesafe"                     % "config"                                 % V.typesafeConfig,
  "io.confluent"                     % "kafka-avro-serializer"                  % V.confluentAvroSerde  % Provided,
  "mysql"                            % "mysql-connector-java"                   % V.jdbcMysql           % Provided,
  "net.java.dev.jna"                 % "jna"                                    % V.jna                 % Test,
  "org.apache.hadoop"                % "hadoop-client"                          % V.hadoop              % Provided,
  "org.apache.iceberg"               % s"iceberg-flink-runtime-${V.flinkMinor}" % V.iceberg             % Provided,
  "org.postgresql"                   % "postgresql"                             % V.jdbcPg              % Provided,
  "org.scalacheck"                  %% "scalacheck"                             % V.scalaCheck,
  "org.scalactic"                   %% "scalactic"                              % V.scalaTest,
  "org.scalatest"                   %% "scalatest"                              % V.scalaTest           % Test,
  "org.scalatestplus"               %% "scalacheck-1-17"                        % V.scalaTestPlus       % Test,
  "org.typelevel"                   %% "squants"                                % V.squants,
  "software.amazon.awssdk"           % "aws-sdk-java"                           % V.awsSdk2             % Test,
  "software.amazon.awssdk"           % "glue"                                   % V.awsSdk2             % Test,
  "software.amazon.awssdk"           % "url-connection-client"                  % V.awsSdk2             % Test,
  "io.dropwizard.metrics"            % "metrics-core"                           % V.dropWizard          % Provided,
  "org.mockito"                     %% "mockito-scala"                          % V.mockito             % Test
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