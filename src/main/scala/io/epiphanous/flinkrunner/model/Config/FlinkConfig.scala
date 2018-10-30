package io.epiphanous.flinkrunner.model.Config
import org.apache.flink.streaming.api.TimeCharacteristic

case class FlinkConfig(
    environment: String,
    debug: Boolean = false,
    timeCharacteristic: TimeCharacteristic = TimeCharacteristic.EventTime,
    parallelism: Int = 1,
    state: StateConfig = StateConfig(),
    restartStrategy: RestartStrategyConfig = FixedDelayConfig(),
    showPlan: Boolean = false,
    mockEdges: Boolean = false,
    sources: Map[String, SourceConfig],
    sinks: Map[String, SinkConfig],
    jobs: Map[String, JobConfig])
