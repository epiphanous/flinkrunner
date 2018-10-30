package io.epiphanous.flinkrunner.model.Config

case class CheckpointsConfig(
    dir: String = "file:///tmp/checkpoints",
    async: Boolean = true,
    exactlyOnce: Boolean = true,
    interval: Option[Int] = Some(30000),
    minPauseBetweenCheckpoints: Option[Int] = None,
    timeout: Int = 60000,
    flash: Boolean = true,
    incremental: Boolean = true,
    retainOnCancellation: Boolean = false,
    maxConcurrent: Int = 1,
    memoryThreshold: Long = 1024,
    failOnError: Boolean = true) {
  def enabled: Boolean = interval.nonEmpty
}
