package io.epiphanous.flinkrunner.model

import org.apache.flink.streaming.api.graph.StreamGraph

import scala.collection.JavaConverters._

case class StreamNodeInfo(
    id: Int,
    name: String,
    parallelism: Int,
    inClasses: List[String],
    outClass: Option[String]) {
  val isSource: Boolean              = inClasses.isEmpty
  val isSink: Boolean                = outClass.isEmpty
  val isTransform: Boolean           = !isSource && !isSink
  val nodeKind: String               =
    if (isSource) "source" else if (isTransform) "transform" else "sink"
  val simpleInClasses: List[String]  =
    inClasses.map(_.split("\\.").last)
  val simpleOutClass: Option[String] = outClass.map(_.split("\\.").last)
}

object StreamNodeInfo {
  def from(sg: StreamGraph): Seq[StreamNodeInfo] = {
    sg.getStreamNodes.asScala.map { sn =>
      val id          = sn.getId
      val name        = sn.getOperatorName
      val parallelism = sn.getParallelism
      val inClasses   = sn.getTypeSerializersIn.toList.map(
        _.createInstance().getClass.getCanonicalName
      )
      val outClass    =
        Option(sn.getTypeSerializerOut)
          .map(_.createInstance().getClass.getCanonicalName)
      StreamNodeInfo(
        id = id,
        name = name,
        parallelism = parallelism,
        inClasses = inClasses,
        outClass = outClass
      )
    }.toSeq
  }
}
