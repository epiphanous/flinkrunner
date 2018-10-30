package io.epiphanous.flinkrunner.model.Config
import scala.concurrent.duration._

sealed trait RestartStrategyConfig

case class NoRestartConfig() extends RestartStrategyConfig
case class FailureRateConfig(
    maxFailuresPerInterval: Int = 1,
    failureRateInterval: FiniteDuration = 1.minute,
    delay: FiniteDuration = 10.seconds)
    extends RestartStrategyConfig
case class FixedDelayConfig(attempts: Int = Integer.MAX_VALUE, delay: FiniteDuration = 10.seconds)
    extends RestartStrategyConfig
