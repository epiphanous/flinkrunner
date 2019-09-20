package io.epiphanous.flinkrunner

import org.scalatest._
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class BasePropSpec extends PropSpec with BaseSpec with ScalaCheckDrivenPropertyChecks with PropGenerators {}
