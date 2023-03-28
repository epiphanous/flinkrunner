package io.epiphanous.flinkrunner.util

import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.model.{BWrapper, SimpleB}

class RowUtilsSpec extends PropSpec {

  property("rowType works") {
    val x = RowUtils.rowTypeOf[BWrapper]
    println(x)
    val z = RowUtils.rowTypeOf[SimpleB]
    println(z)
  }
}
