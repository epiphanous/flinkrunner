package io.epiphanous.flinkrunner.model.Config

case class StateConfig(
    backend: StateBackendEnum = StateBackendEnum.RocksDB,
    checkpoints: CheckpointsConfig = CheckpointsConfig(),
    savepoints: SavepointsConfig = SavepointsConfig())
