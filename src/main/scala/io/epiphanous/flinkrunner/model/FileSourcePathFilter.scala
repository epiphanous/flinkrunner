package io.epiphanous.flinkrunner.model

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.source.FileSourceConfig
import org.apache.flink.api.common.io.{FilePathFilter, GlobFilePathFilter}
import org.apache.flink.core.fs.Path

import java.util
import java.util.function.Predicate
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class FileSourcePathFilter[ADT <: FlinkEvent](
    sourceConfig: FileSourceConfig[ADT])
    extends FilePathFilter
    with Predicate[Path]
    with LazyLogging {

  lazy val includePaths: util.List[String] = processIncludePaths(
    sourceConfig.includePaths
  )

  def processIncludePaths(paths: List[String]): util.List[String] =
    paths
      .foldLeft(ArrayBuffer.empty[String]) {
        (out: ArrayBuffer[String], path: String) =>
          out ++ (path zip path.view.tail).zipWithIndex
            .filter { case ((c0, c1), _) => c0 != '*' && c1 == '/' }
            .map(_._2 + 1)
            .foldLeft(ArrayBuffer.empty[String]) { case (b, i) =>
              val prefix = path.substring(0, i)
              if (!prefix.equals("**")) b += prefix
              b
            } :+ path
      }
      .toList
      .asJava

  lazy val globFilePathFilter = new GlobFilePathFilter(
    includePaths,
    sourceConfig.excludePaths.asJava
  )

  lazy val hiddenFilePredicate: Predicate[Path] = (path: Path) => {
    val fileName = path.getName
    Option(fileName).isEmpty ||
    fileName.isEmpty ||
    fileName.startsWith(".") ||
    fileName.startsWith("_")
  }

  /** Return true if filePath should be ignored, or false if it should be
    * processed
    * @param filePath
    *   the file path to test
    * @return
    *   Boolean
    */
  def _filter(filePath: Path): Boolean = {
    val isHiddenFile  = hiddenFilePredicate.test(
      filePath
    )
//    val isDir         = filePath.getFileSystem.getFileStatus(filePath).isDir
    val shouldExclude = globFilePathFilter.filterPath(filePath)
    if (!(isHiddenFile || shouldExclude)) logger.debug(filePath.getPath)
    isHiddenFile || shouldExclude
  }

  override def filterPath(filePath: Path): Boolean = _filter(filePath)

  override def test(filePath: Path): Boolean = _filter(filePath)
}
