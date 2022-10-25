package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.model.source.FileSourceConfig
import org.apache.flink.connector.file.src.enumerate.{
  FileEnumerator,
  NonSplittingRecursiveEnumerator
}

case class FileSourceEnumeratorProvider[ADT <: FlinkEvent](
    sourceConfig: FileSourceConfig[ADT])
    extends FileEnumerator.Provider {
  override def create(): FileEnumerator =
    new NonSplittingRecursiveEnumerator(sourceConfig.fileFilter)
}
