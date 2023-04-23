package io.epiphanous.flinkrunner.model

case class DataTypeConfig(
    value: Option[String] = None,
    bridgedTo: Option[Class[_]] = None,
    defaultDecimalPrecision: Option[Int] = None,
    defaultDecimalScale: Option[Int] = None,
    defaultSecondPrecision: Option[Int] = None,
    defaultYearPrecision: Option[Int] = None)
