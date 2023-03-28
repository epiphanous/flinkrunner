package io.epiphanous.flinkrunner.util

import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.model.SimpleA

class AttributesOfSpec extends PropSpec {

  property("works") {
    val a = genOne[SimpleA]
    val f = Fields.of(
      a
    ) // <-- ignore this implicit error in intellij, compiler is ok
    f shouldEqual List(
      ("id", a.id),
      ("a0", a.a0),
      ("a1", a.a1),
      ("ts", a.ts)
    )
  }
}
