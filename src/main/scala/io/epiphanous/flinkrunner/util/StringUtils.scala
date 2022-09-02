package io.epiphanous.flinkrunner.util

trait StringUtils {

  def snakify(name: String): String =
    name
      .replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2")
      .replaceAll("([a-z\\d])([A-Z])", "$1_$2")
      .toLowerCase

  def clean(
      in: String,
      cleanChars: String = "a-zA-Z0-9_",
      replacement: String = ""): String =
    if (in == null) "" else in.replaceAll(s"[^$cleanChars]", replacement)

}
