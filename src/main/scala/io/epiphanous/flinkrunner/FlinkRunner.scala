package io.epiphanous.flinkrunner

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.flink.{FlinkArgDef, FlinkJob, FlinkJobObject}
import io.epiphanous.flinkrunner.model.FlinkEvent

/**
  * Flink Job Invoker
  */
class FlinkRunner[E <: FlinkEvent](systemName: String, jobs: Map[String, (FlinkJob[E], FlinkJobObject)])
    extends LazyLogging {

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
      case _                                         => false
    }
    jobs.get(jobName) match {
      case Some((job, obj)) =>
        if (wantsHelp) showHelpFor(jobName)
        else
          job.run(jobName, args, obj.extraArgs) match {
            case Left(results) => callback(results)
            case Right(_)      => ()
          }
      case None =>
        showHelp(Some(s"Unknown job $jobName"))
    }
  }

  /**
    * Show help for a particular job
    *
    * @param jobName name of the job to get help for
    */
  def showHelpFor(jobName: String): Unit = {
    jobs.get(jobName) match {
      case Some((_, obj)) =>
        val params   = (FlinkArgDef.CORE_FLINK_ARGUMENTS ++ obj.extraArgs).map(a => a.name -> a).toMap
        val names    = params.keys.toList.sorted
        val maxWidth = names.map(_.length).max

        def pad(s: String) = s"$s${" " * (maxWidth - s.length)}"
        def showDefault(s: Option[String]) = s match {
          case Some(x) if !x.isEmpty => s"(default: $x)"
          case _                     => ""
        }

        val paramInfo = names
          .map(arg => {
            s"${pad(arg)}  ${params(arg).text} ${showDefault(params(arg).default)}"
          })
          .mkString("  - ", "\n  - ", "\n")
        val usage =
          s"""
             |Usage: $systemName $jobName [job parameters]
             |${obj.jobDescription}
             |Job Parameters:
             |
             |$paramInfo
       """.stripMargin
        println(usage)
      case None => logger.error(s"Unknown job $jobName")
    }
  }

  /**
    * Show a help message regarding usage.
    *
    * @param error an optional error message to show
    */
  def showHelp(error: Option[String] = None): Unit = {
    // todo: complete this
    val jobList = jobs.keys.toList.sorted.mkString("  - ", "\n  - ", "\n")
    val usage =
      s"""
         |Usage: $systemName <jobName> [job parameters]
         |
         |  Jobs (try "$systemName <jobName> help" for details)
         |  -------------------------------------------------
         |$jobList
      """.stripMargin
    error match {
      case Some(errMsg) => logger.error(errMsg)
      case _            => // no op
    }
    println(usage)
  }

}
