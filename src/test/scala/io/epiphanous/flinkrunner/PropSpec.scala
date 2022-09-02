package io.epiphanous.flinkrunner

import org.scalatest.propspec.AnyPropSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class PropSpec
    extends AnyPropSpec
    with BaseSpec
    with ScalaCheckDrivenPropertyChecks
    with PropGenerators {}
