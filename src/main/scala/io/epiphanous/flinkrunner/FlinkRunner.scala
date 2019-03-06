package io.epiphanous.flinkrunner

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkEvent}

/**
  * Flink Job Invoker
  */
class FlinkRunner[ADT <: FlinkEvent](
  args: Array[String],
  factory: FlinkRunnerFactory[ADT],
  sources: Map[String, Seq[Array[Byte]]] = Map.empty,
  optConfig: Option[String] = None)
    extends LazyLogging {

  implicit val config: FlinkConfig = new FlinkConfig(args, factory, sources, optConfig)
  implicit val env: SEE = config.configureStreamExecutionEnvironment

  /**
    * An intermediate method to process main args, with optional callback to
    * capture output of flink job.
    *
    * @param callback a function from an iterator to unit
    */
  def process(
    callback: PartialFunction[Stream[ADT], Unit] = {
      case _ => ()
    }
  ): Unit =
    if (config.jobName == "help") showHelp()
    else process1(callback)

  /**
    * Actually invoke the job based on the job name and arguments passed in.
    * If the job run returns an iterator of results, pass those results to the
    * callback. Otherwise, just return. The callback is for testing the stream
    * of results from a flink job. It will only be invoked if --mock.edges
    * option is on.
    *
    * @param callback a function from a stream to unit that receives results
    *                 from running flink job
    */
  def process1(
    callback: PartialFunction[Stream[ADT], Unit] = {
      case _ => ()
    }
  ): Unit = {
    if (config.jobArgs.headOption.exists(s => List("help", "--help", "-help", "-h").contains(s))) showJobHelp()
    else {
      config.getJobInstance.run match {
        case Left(results) => callback(results.asInstanceOf[Iterator[ADT]].toStream)
        case Right(_)      => ()
      }
    }
  }

  /**
    * Show help for a particular job
    **/
  def showJobHelp(): Unit = {
    val usage =
      s"""|
          |Usage: ${config.systemName} ${config.jobName} [job parameters]
          |${config.jobHelp}
       """.stripMargin
    println(usage)
  }

  /**
    * Show a help message regarding usage.
    *
    * @param error an optional error message to show
    */
  def showHelp(error: Option[String] = None): Unit = {
    val jobInfo = config.jobs.toList.sorted match {
      case s if s.isEmpty => "  *** No jobs defined ***"
      case s =>
        s.map(jn => {
            val desc = config.getString(s"jobs.$jn.description")
            s"  - $jn: $desc"
          })
          .mkString("\n")
    }
    val usage =
      s"""|
          |Usage: ${config.systemName} <jobName> [job parameters]
          |
          |Jobs:
          |
          |$jobInfo
          |
          |Try "${config.systemName} <jobName> --help" for details)
          |${config.systemHelp}
      """.stripMargin
    error.foreach(m => logger.error(m))
    println(usage)
  }

}
