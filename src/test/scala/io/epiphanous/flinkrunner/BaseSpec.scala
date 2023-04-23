package io.epiphanous.flinkrunner

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.util.test.FlinkRunnerSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{EitherValues, Inside, OptionValues, TryValues}

trait BaseSpec
    extends Matchers
    with OptionValues
    with EitherValues
    with TryValues
    with Inside
    with FlinkRunnerSpec
    with LazyLogging {}
