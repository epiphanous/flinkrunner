name := "flinkrunner"

lazy val scala212               = "2.12.12"
lazy val scala211               = "2.11.12"
lazy val supportedScalaVersions = List(scala212)

inThisBuild(
  List(
    organization := "io.epiphanous",
    homepage := Some(url("https://github.com/epiphanous/flinkrunner")),
    licenses := List("MIT" -> url("https://opensource.org/licenses/MIT")),
    developers := List(
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

Test / parallelExecution := false
Test / fork := true
resolvers += "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository"
resolvers += "Confluent Repository" at "https://packages.confluent.io/maven/"

val V = new {
  val flink              = "1.14.2"
  val logback            = "1.2.7"
  val scalaLogging       = "3.9.4"
  val scalaTest          = "3.2.10"
  val scalaCheck         = "3.2.9.0"
  val circe              = "0.14.1"
  val http4s             = "0.21.29"
  val enumeratum         = "1.7.0"
  val typesafeConfig     = "1.4.1"
  val guava              = "31.0.1-jre" //"29.0-jre"
  val squants            = "1.8.3"
  val avro4s             = "4.0.11"
  val confluentAvroSerde = "7.0.1"
}

val flinkDeps =
  Seq(
    "org.apache.flink" %% "flink-scala"                    % V.flink % Provided,
    "org.apache.flink" %% "flink-streaming-scala"          % V.flink % Provided,
    "org.apache.flink" %% "flink-cep-scala"                % V.flink % Provided,
    "org.apache.flink" %% "flink-table-planner"            % V.flink % Provided,
    "org.apache.flink" %% "flink-connector-kafka"          % V.flink,
    "org.apache.flink" %% "flink-connector-kinesis"        % V.flink,
    "org.apache.flink" %% "flink-connector-cassandra"      % V.flink,
    "org.apache.flink" %% "flink-connector-elasticsearch7" % V.flink,
    "org.apache.flink" %% "flink-connector-jdbc"           % V.flink,
    "org.apache.flink" %% "flink-connector-rabbitmq"       % V.flink,
    "org.apache.flink"  % "flink-connector-files"          % V.flink,
    "org.apache.flink" %% "flink-table-api-scala-bridge"   % V.flink,
    "org.apache.flink" %% "flink-statebackend-rocksdb"     % V.flink,
    "org.apache.flink"  % "flink-avro-confluent-registry"  % V.flink,
    "org.apache.flink" %% "flink-test-utils"               % V.flink % Test
  )

val loggingDeps = Seq(
  "ch.qos.logback"              % "logback-classic" % V.logback % Provided,
  "com.typesafe.scala-logging" %% "scala-logging"   % V.scalaLogging
)

val http4sDeps = Seq(
  "dsl",
  "client",
  "blaze-client",
  "circe"
).map(d => "org.http4s" %% s"http4s-$d" % V.http4s)

val circeDeps  = Seq(
  "core",
  "generic",
  "generic-extras",
  "parser"
).map(d => "io.circe" %% s"circe-$d" % V.circe)

val otherDeps  = Seq(
  "io.confluent"         % "kafka-streams-avro-serde" % V.confluentAvroSerde,
  "com.beachape"        %% "enumeratum"               % V.enumeratum,
  "com.typesafe"         % "config"                   % V.typesafeConfig,
  "com.google.guava"     % "guava"                    % V.guava,
  "org.typelevel"       %% "squants"                  % V.squants,
  "com.sksamuel.avro4s" %% "avro4s-core"              % V.avro4s,
  "org.scalactic"       %% "scalactic"                % V.scalaTest  % Test,
  "org.scalatest"       %% "scalatest"                % V.scalaTest  % Test,
  "org.scalatestplus"   %% "scalacheck-1-15"          % V.scalaCheck % Test
)

/**
 * Exclude any transitive deps using log4j
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

lazy val flink_runner =
  (project in file("."))
    .settings(
      crossScalaVersions := supportedScalaVersions,
      libraryDependencies ++=
        (flinkDeps ++ http4sDeps ++ circeDeps ++ otherDeps)
          .map(excludeLog4j)
          ++ loggingDeps
    )

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
Global / cancelable := true

Compile / run := Defaults
  .runTask(
    Compile / fullClasspath,
    Compile / run / mainClass,
    Compile / run / runner
  )
  .evaluated
