package io.epiphanous.flinkrunner.flink
import io.epiphanous.flinkrunner.model.FlinkEvent
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream

class IdentityJob[E <: FlinkEvent: TypeInformation](sources: Map[String, Seq[Array[Byte]]] = Map.empty)
    extends SimpleFlinkJob[E, E](sources) {

  override def transform(in: DataStream[E])(implicit args: Args, env: SEE) =
    in
}
