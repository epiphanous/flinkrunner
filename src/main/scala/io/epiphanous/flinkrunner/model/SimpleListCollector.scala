package io.epiphanous.flinkrunner.model

import org.apache.flink.api.common.functions.util.ListCollector
import org.apache.flink.util.Collector

import java.util
import scala.collection.JavaConverters._

/** A simple flink collector, useful for testing
  * @tparam T
  */
class SimpleListCollector[T] extends Collector[T] {

  private val collected: util.ArrayList[T] = new util.ArrayList[T]()

  private val underlyingCollector: ListCollector[T] =
    new ListCollector[T](collected)

  override def collect(record: T): Unit =
    underlyingCollector.collect(record)

  /** Return the collected objects as a scala list
    * @return
    *   List[T]
    */
  def getCollectedObjects: List[T] = collected.asScala.toList

  /** Return the size of the object collection */
  def size: Int = collected.size()

  /** Return the length of the object collection (alias for size) */
  def length: Int = size

  override def close(): Unit = underlyingCollector.close()
}
