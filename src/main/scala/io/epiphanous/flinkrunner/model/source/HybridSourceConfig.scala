package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.model.{
  FlinkConfig,
  FlinkConnectorName,
  FlinkEvent
}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.{Source, SourceSplit}
import org.apache.flink.connector.base.source.hybrid.HybridSource
import org.apache.flink.streaming.api.functions.source.SourceFunction

/** A hybrid source configuration.
  * @param name
  *   name of the hybrid source
  * @param config
  *   a flinkrunner configuration
  * @param connector
  *   [[FlinkConnectorName.Hybrid]]
  * @tparam ADT
  *   the flinkrunner algebraic data type
  */
case class HybridSourceConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig,
    connector: FlinkConnectorName = FlinkConnectorName.Hybrid)
    extends SourceConfig[ADT] {

  def getSources[E <: ADT: TypeInformation]
      : List[Source[E, _ <: SourceSplit, _]] =
    config
      .getStringList(pfx("sources"))
      .map(name =>
        SourceConfig[ADT](name, config).getSource[E] match {
          case Right(source) => source
          case Left(_)       =>
            throw new RuntimeException(
              s"Source $name can't used in a hybrid source as it's based on Flink's SourceFunction API. Hybrid sources require the new Source API."
            )
        }
      )

  override def getSource[E <: ADT: TypeInformation]
      : Either[SourceFunction[E], Source[E, _ <: SourceSplit, _]] = {
    val sources = getSources[E]
    if (sources.size < 2)
      throw new RuntimeException(
        s"hybrid source $name requires at least two constituent sources"
      )
    val h       = HybridSource.builder(sources.head)
    val hsb     = sources.tail.foldLeft(h) { case (hsb, src) =>
      hsb.addSource(src)
    }
    Right(hsb.build())
  }

}
