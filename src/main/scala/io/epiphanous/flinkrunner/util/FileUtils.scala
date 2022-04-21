package io.epiphanous.flinkrunner.util

import java.io.{File, FileNotFoundException}
import scala.util.matching.Regex

object FileUtils {

  val RESOURCE_PATTERN: Regex = "resource:///?(.*)".r

  /**
   * Returns the actual path to a resource file named filename or
   * filename.gz.
   *
   * @param filename
   *   the name of file
   * @return
   *   String
   */
  @throws[FileNotFoundException]
  def getResource(filename: String): String = {
    val loader   = getClass.getClassLoader
    val resource = Option(
      loader.getResource(filename)
    ) match {
      case Some(value) => value.toURI
      case None        =>
        Option(loader.getResource(s"$filename.gz")) match {
          case Some(value) => value.toURI
          case None        =>
            throw new FileNotFoundException(
              s"can't load resource $filename"
            )
        }
    }
    val file     = new File(resource)
    file.getAbsolutePath
  }

  @throws[FileNotFoundException]
  def getResourceOrFile(path: String): String = path match {
    case RESOURCE_PATTERN(p) => getResource(p)
    case other               => other
  }
}
