package io.epiphanous.flinkrunner.util

import io.epiphanous.flinkrunner.PropSpec

class StringUtilsTest extends PropSpec with StringUtils {

  property("snakify") {
    val s         = "thisIsACamelCasedString"
    val snakified = "this_is_a_camel_cased_string"
    snakify(s) shouldEqual snakified
  }

  property("clean") {
    val s       = "$this_$Is_a-dirty#%$string"
    val cleaned = "this_Is_adirtystring"
    clean(s) shouldEqual cleaned
  }

}
