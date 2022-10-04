package io.epiphanous.flinkrunner.model.sink

import com.dimafeng.testcontainers.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName

object TimescaleDbContainer {
  def apply(): PostgreSQLContainer = PostgreSQLContainer(
    dockerImageNameOverride = DockerImageName
      .parse("timescale/timescaledb")
      .withTag("latest-pg14")
      .asCompatibleSubstituteFor("postgres")
  )
}