package io.epiphanous.flinkrunner

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

package object flink {
  type SEE  = StreamExecutionEnvironment
  type Args = FlinkJobArgs
}
