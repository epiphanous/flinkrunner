package io.epiphanous.flinkrunner.flink
import io.epiphanous.flinkrunner.SEE
import io.epiphanous.flinkrunner.model.{FlinkConfig, FlinkEvent}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream

class IdentityJob[E <: FlinkEvent: TypeInformation] extends FlinkJob[E, E] {

  override def transform(in: DataStream[E])(implicit config: FlinkConfig, env: SEE) =
    in
}
