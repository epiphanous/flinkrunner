package io.epiphanous.flinkrunner.flink
import io.epiphanous.flinkrunner.model.FlinkEvent

import scala.reflect.runtime.universe

trait FlinkJobTest {

  lazy val _runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)

  def runJob[E <: FlinkEvent](job: FlinkJob[E], argString: String): Seq[E] = {
    val jobClass  = job.getClass
    val jobObject = getJobObject(jobClass.getName)
    val jobName   = jobClass.getSimpleName
    val argList   = argString.split("\\s+").toList
    val args      = if (argString.isEmpty || argString.startsWith("--")) jobName :: argList else argList
    job.run(args.head, args.tail.toArray, jobObject.extraArgs) match {
      case Left(iter: Iterator[E]) => iter.toSeq
      case _                       => Seq.empty[E]
    }
  }

  def getJobObject(jobName: String): FlinkJobObject = {
    val module = _runtimeMirror.staticModule(jobName)
    _runtimeMirror.reflectModule(module).instance.asInstanceOf[FlinkJobObject]
  }

}
