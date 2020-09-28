package io.epiphanous.flinkrunner

import com.typesafe.scalalogging.LazyLogging
import org.scalatest._
import org.scalatest.matchers.should.Matchers

trait BaseSpec extends Matchers with OptionValues with EitherValues with TryValues with Inside with LazyLogging
