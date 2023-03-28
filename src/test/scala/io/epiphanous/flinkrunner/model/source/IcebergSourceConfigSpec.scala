package io.epiphanous.flinkrunner.model.source

import com.dimafeng.testcontainers.lifecycle.and
import io.epiphanous.flinkrunner.model.{IcebergConfigSpec, SimpleB}

class IcebergSourceConfigSpec extends IcebergConfigSpec {

  val bRows: Seq[SimpleB] = genPop[SimpleB]()

  override def afterContainersStart(containers: Containers): Unit = {
    super.afterContainersStart(containers)
    containers match {
      case ls and ib =>
    }
  }

  property("iceberg source works") {}
}
