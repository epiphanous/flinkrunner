package io.epiphanous.flinkrunner.flink.state
import scala.collection.JavaConverters._

object RichStateUtils {
  implicit class RichListState[T](listState: org.apache.flink.api.common.state.ListState[T]) {
    def _iterator: Iterator[T] = listState.get().iterator().asScala
    def isEmpty: Boolean = _iterator.isEmpty
    def contains(element: T): Boolean = _iterator.contains(element)
    def find(element: T) : T = _iterator.find( v => v.equals(element)).get
    def length: Int = _iterator.length
  }
}
