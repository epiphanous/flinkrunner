package io.epiphanous.flinkrunner

import org.scalatest.propspec.AnyPropSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PropSpec
    extends AnyPropSpec
    with BaseSpec
    with ScalaCheckPropertyChecks
    with PropGenerators {}
