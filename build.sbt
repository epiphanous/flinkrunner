name := "flinkrunner"

lazy val scala212 = "2.12.12"
lazy val scala211 = "2.11.12"
lazy val supportedScalaVersions = List(scala212, scala211)

inThisBuild(
  List(organization := "io.epiphanous",
       homepage := Some(url("https://github.com/epiphanous/flinkrunner")),
       licenses := List("MIT" -> url("https://opensource.org/licenses/MIT")),
       developers := List(Developer("nextdude", "Robert Lyons", "nextdude@gmail.com", url("https://epiphanous.io"))),
       scalaVersion := scala212)
)

Test / parallelExecution := false
Test / fork := true
resolvers += "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository"

val V = new {
  val flink = "1.11.0"
  val logback = "1.2.3"
  val log4jOverSlf4j = "1.7.30"
  val scalaLogging = "3.9.2"
  val scalaTest = "3.2.0"
  val scalaCheck = "1.14.3"
  val circe = "0.13.0"
  val http4s = "0.21.6"
  val enumeratum = "1.6.1"
  val typesafeConfig = "1.3.4"
  val guava = "29.0-jre"
  val squants = "1.6.0"
  val avro = "1.10.0"
  val avro4s = "4.0.0-RC1"
  val avro4s_211 = "3.0.0-RC2"
}

val flinkDeps = (
  Seq("scala", "streaming-scala", "cep-scala").map(
    a => "org.apache.flink" %% s"flink-$a" % V.flink % Provided
  ) ++
    Seq("connector-kafka", "connector-kinesis", "connector-cassandra",
      "connector-elasticsearch7", "statebackend-rocksdb").map(a => "org.apache.flink" %% s"flink-$a" % V.flink) ++
    Seq("org.apache.flink" %% "flink-test-utils" % V.flink % Test)
).map(
  _.excludeAll(ExclusionRule(organization = "log4j"), ExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12"))
)

val loggingDeps = Seq("ch.qos.logback"             % "logback-core"     % V.logback % Provided,
                      "ch.qos.logback"             % "logback-classic"  % V.logback % Provided,
                      "org.slf4j"                  % "log4j-over-slf4j" % V.log4jOverSlf4j % Provided,
                      "com.typesafe.scala-logging" %% "scala-logging"   % V.scalaLogging)

val http4sDeps =
  Seq("http4s-dsl", "http4s-client", "http4s-blaze-client", "http4s-circe").map("org.http4s" %% _ % V.http4s)

val circeDeps = Seq("circe-core", "circe-generic", "circe-generic-extras", "circe-parser").map(
  "io.circe" %% _ % V.circe
)

val otherDeps = Seq("com.beachape"        %% "enumeratum"  % V.enumeratum,
                    "org.apache.avro"     % "avro"         % V.avro,
                    "com.typesafe"        % "config"       % V.typesafeConfig,
                    "com.google.guava"    % "guava"        % V.guava,
                    "org.typelevel"       %% "squants"     % V.squants,
                    "org.scalactic"       %% "scalactic"   % V.scalaTest % Test,
                    "org.scalatest"       %% "scalatest"   % V.scalaTest % Test,
                    "org.scalacheck"      %% "scalacheck"  % V.scalaCheck % Test)

lazy val flink_runner =
  (project in file("."))
    .settings(crossScalaVersions := supportedScalaVersions,
              libraryDependencies ++=
                flinkDeps ++ loggingDeps ++ http4sDeps ++ circeDeps ++ otherDeps ++ {
                  CrossVersion.partialVersion(scalaVersion.value) match {
                    case Some((2, scalaMinor)) if scalaMinor == 11 => Seq("com.sksamuel.avro4s" %% "avro4s-core" % V.avro4s_211)
                    case _ => Seq("com.sksamuel.avro4s" %% "avro4s-core" % V.avro4s)
                  }
                })

scalacOptions ++= Seq("-encoding",
                      "utf8",
                      "-deprecation",
                      "-Xfuture",
                      "-Ywarn-dead-code",
                      "-Ywarn-numeric-widen",
                      "-Ywarn-unused",
                      "-Ywarn-value-discard")

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

run in Compile := Defaults
  .runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
  .evaluated
