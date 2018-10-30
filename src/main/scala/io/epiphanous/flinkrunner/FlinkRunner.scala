package io.epiphanous.flinkrunner

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.flink.{FlinkArgDef, FlinkJob, FlinkJobObject}
import io.epiphanous.flinkrunner.model.Config.{DeserializerCoproduct, FlinkSystem, JobConfig, SinkConfig}
import io.epiphanous.flinkrunner.model.FlinkEvent

/**
  * Flink Job Invoker
  */
class FlinkRunner[E <: FlinkEvent](jobFactory:String => FlinkJob[E], deserializers:SinkConfig => DeserializerCoproduct) extends LazyLogging {

  val system =
    pureconfig.loadConfig[FlinkSystem] match {
      case Right(c) => c
      case Left(t) => throw new RuntimeException(t.toList.map(_.toString).mkString("\n"))
    }

  /**
    * An intermediate method to process main args, with optional callback to
    * capture output of flink job.
    *
    * @param args     Array[String] arguments at program invocation
    * @param callback a function from an iterator to unit
    */
  def process(
      args: Array[String],
      callback: PartialFunction[Iterator[E], Unit] = {
        case _ => ()
      }
    ): Unit = {
    def badJobName(j: String) = j.equalsIgnoreCase("help") || j.startsWith("-")

    args.headOption match {
      case Some(jobName) if !badJobName(jobName) =>
        process1(jobName, args.slice(1, args.length), callback)
      case _ => showHelp()
    }
  }

  /**
    * Actually invoke the job based on the job name and arguments passed in.
    * If the job run returns an iterator of results, pass those results to the
    * callback. Otherwise, just return. The callback is for testing the stream
    * of results from a flink job. It will only be invoked if --mock.edges
    * option is on.
    *
    * @param jobName  String first argument at program invocation
    * @param args     Array[String] other arguments at program invocation
    * @param callback a function from an iterator to unit that receives results
    *                 from running flink job
    */
  def process1(
      jobName: String,
      args: Array[String],
      callback: PartialFunction[Iterator[E], Unit] = {
        case _ => ()
      }
    ): Unit = {
    val wantsHelp = args.headOption match {
      case Some(str) if str.equalsIgnoreCase("help") => true
      case _ => false
    }
    system.jobs.get(jobName) match {
      case Some(jobConfig) =>
        if (wantsHelp) showHelpFor(jobConfig)
        else
          val job = jobFactory(jobName)
          job.run(args, system, jobConfig) match {
            case Left(results) => callback(results)
            case Right(_) => ()
          }
      case None =>
        showHelp(Some(s"Unknown job $jobName"))
    }
  }

  /**
    * Show help for a particular job
    *
    * @param job job config object
    */
  def showHelpFor(job: JobConfig): Unit = {
    val usage =
      s"""|Usage: ${ system.name} ${job.name} [job parameters]
          |
          |${job.help}
       """.stripMargin
    println(usage)
  }

  /**
    * Show a help message regarding usage.
    *
    * @param error an optional error message to show
    */
  def showHelp(error: Option[String] = None): Unit = {
    val usage =
      s"""
         |Usage: ${system.name} <jobName> [job parameters]
         |
         |Jobs (try "${system.name} <jobName> help" for details)
         |
         |  - ${system.envConfig.jobs.map { case (key, job) => s"${job.name} - ${job.description}" }.mkString("\n  - ")}
      """.stripMargin
    error match {
      case Some(errMsg) => logger.error(errMsg)
      case _ => // no op
    }
    println(usage)
  }

}
