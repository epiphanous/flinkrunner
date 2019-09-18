name := "flinkrunner"

inThisBuild(List(
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
  )
))

ThisBuild / scalaVersion := "2.11.12"

Test / parallelExecution := false

Test / fork := true

resolvers += "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository"

val V = new {
  val flink = "1.8.2"
  val logback = "1.2.3"
  val log4jOverSlf4j = "1.7.26"
  val scalaLogging = "3.9.2"
  val scalaTest = "3.0.8"
  val circe = "0.11.1"
  val http4s = "0.20.10"
  val enumeratum = "1.5.13"
  val typesafeConfig = "1.3.4"
  //  val guava = "27.0.1-jre"
  val guava = "24.1-jre"
  val squants = "1.3.0"
  val antlr4 = "4.7.1"
}


enablePlugins(Antlr4Plugin)
antlr4Version in Antlr4 := V.antlr4
antlr4PackageName in Antlr4 := Some("io.epiphanous.antlr4")

val withK = Seq("true","1","yes","y").exists(
  _.equalsIgnoreCase(System.getProperty("with.kinesis", "false"))
)

val maybeKinesis = if (withK) Seq("connector-kinesis") else Seq.empty[String]

// post-process version to add k suffix if we're building with kinesis
val versionSuffix = if (withK) "k" else ""
version in ThisBuild ~= (v => v.replaceFirst("^(v?\\d(\\.\\d){2})(?=[^k])",s"$$1$versionSuffix") + versionSuffix)
dynver in ThisBuild ~= (v => v.replaceFirst("^(v?\\d(\\.\\d){2})(?=[^k])",s"$$1$versionSuffix") + versionSuffix)

val flinkDeps = (
  (Seq("scala", "streaming-scala", "cep-scala") ++ maybeKinesis).map(a =>
    "org.apache.flink" %% s"flink-$a" % V.flink % Provided
  ) ++
  Seq("connector-kafka", "statebackend-rocksdb").map(a =>
    "org.apache.flink" %% s"flink-$a" % V.flink
  ) ++
  Seq(
    "org.apache.flink" %% "flink-test-utils" % V.flink % Test
  )
  ).map(
  _.excludeAll(ExclusionRule(organization = "log4j"), ExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12"))
)

val loggingDeps = Seq("ch.qos.logback"             % "logback-core"     % V.logback % Provided,
                      "ch.qos.logback"             % "logback-classic"  % V.logback % Provided,
                      "org.slf4j"                  % "log4j-over-slf4j" % V.log4jOverSlf4j % Provided,
                      "com.typesafe.scala-logging" %% "scala-logging"   % V.scalaLogging)

val http4sDeps =
  Seq("http4s-dsl", "http4s-client", "http4s-blaze-client", "http4s-circe").map("org.http4s" %% _ % V.http4s)

val otherDeps = Seq("com.beachape"      %% "enumeratum" % V.enumeratum,
                    "com.typesafe"      %  "config"     % V.typesafeConfig,
                    "com.google.guava"  %  "guava"      % V.guava,
                    "org.typelevel"     %% "squants"    % V.squants,
                    "org.scalactic"     %% "scalactic"  % V.scalaTest % Test,
                    "org.scalatest"     %% "scalatest"  % V.scalaTest % Test)

lazy val flink_runner =
  (project in file(".")).settings(libraryDependencies ++= flinkDeps ++ loggingDeps ++ http4sDeps ++ otherDeps)

scalacOptions ++= Seq(
  "-encoding","utf8",
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

run in Compile := Defaults
  .runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
  .evaluated
