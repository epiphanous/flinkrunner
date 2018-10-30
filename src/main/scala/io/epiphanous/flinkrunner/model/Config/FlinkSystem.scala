package io.epiphanous.flinkrunner.model.Config

case class FlinkSystem(name: String, adt: String, environments: Map[String, FlinkConfig]) {
  import FlinkSystem._
  def env: String = Option(System.getenv(FLINK_RUNNER_ENV)).getOrElse(DEV_ENV)
  def config: FlinkConfig = environments(env)
  def mockEdges = env == DEV_ENV && config.mockEdges
  def sinks = config.sinks
  def sources = config.sources
  def jobs = config.jobs
}

object FlinkSystem {
  final val FLINK_RUNNER_ENV = "FLINK_RUNNER_ENV"
  final val DEV_ENV = "dev"
}
