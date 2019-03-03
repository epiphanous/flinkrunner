name := "flinkrunner"

inThisBuild(List(
  organization := "io.epiphanous"
  homepage := Some(url("https://github.com/epiphanous/flinkrunner")),
  licenses := List("MIT" -> url("https://opensource.org/licenses/MIT")),
  developers := List(
    Developer(
      "nextdude",
      "Robert Lyons",
      "nextdude@gmail.com",
      url("https://epiphanous.io")
    )
  )
))

ThisBuild / scalaVersion := "2.11.12"

Test / parallelExecution := false

Test / fork := true

resolvers += "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository"

val V = new {
  val flink = "1.7.2"
  val logback = "1.2.3"
  val log4jOverSlf4j = "1.7.25"
  val scalaLogging = "3.9.0"
  val scalaTest = "3.0.5"
  val rocksdb = "5.17.2"
  val circe = "0.11.1"
  val http4s = "0.20.0-M5"
  val bloom = "0.11.0-rfl"
  val enumeratum = "1.5.13"
  val typesafeConfig = "1.3.3"
  val guava = "27.0.1-jre"
  val squants = "1.3.0"
  val antlr4 = "4.7.1"
}

enablePlugins(Antlr4Plugin)
antlr4Version in Antlr4 := V.antlr4
antlr4PackageName in Antlr4 := Some("io.epiphanous.antlr4")

val flinkDeps = Seq("org.apache.flink" %% "flink-scala"                % V.flink % "provided",
                    "org.apache.flink" %% "flink-streaming-scala"      % V.flink % "provided",
                    "org.apache.flink" % "flink-s3-fs-hadoop"          % V.flink % "provided",
                    "org.apache.flink" %% "flink-cep-scala"            % V.flink % "provided",
                    "org.apache.flink" %% "flink-connector-kafka"      % V.flink,
                    "org.apache.flink" %% "flink-connector-kinesis"    % V.flink,
                    "org.apache.flink" %% "flink-statebackend-rocksdb" % V.flink,
//                    "org.apache.flink" % "flink-jdbc"                  % V.flink,
                    "org.rocksdb"      % "rocksdbjni"                  % V.rocksdb,
                    "org.apache.flink" %% "flink-test-utils"           % V.flink % "test").map(
  _.excludeAll(ExclusionRule(organization = "log4j"), ExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12"))
)

val loggingDeps = Seq("ch.qos.logback"             % "logback-core"     % V.logback % "provided",
                      "ch.qos.logback"             % "logback-classic"  % V.logback % "provided",
                      "org.slf4j"                  % "log4j-over-slf4j" % V.log4jOverSlf4j % "provided",
                      "com.typesafe.scala-logging" %% "scala-logging"   % V.scalaLogging)

val http4sDeps =
  Seq("http4s-dsl", "http4s-client", "http4s-blaze-client", "http4s-circe").map("org.http4s" %% _ % V.http4s)

val otherDeps = Seq("com.github.ponkin" % "bloom-core"  % V.bloom,
                    "com.beachape"      %% "enumeratum" % V.enumeratum,
                    "com.typesafe"      % "config"      % V.typesafeConfig,
                    "com.google.guava"  % "guava"       % V.guava,
                    "org.typelevel"     %% "squants"    % V.squants,
                    "org.scalactic"     %% "scalactic"  % V.scalaTest % Test,
                    "org.scalatest"     %% "scalatest"  % V.scalaTest % Test)

lazy val flink_runner =
  (project in file(".")).settings(libraryDependencies ++= flinkDeps ++ loggingDeps ++ http4sDeps ++ otherDeps)

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

run in Compile := Defaults
  .runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
  .evaluated
