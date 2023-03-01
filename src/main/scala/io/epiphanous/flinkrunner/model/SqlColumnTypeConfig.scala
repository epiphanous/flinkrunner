package io.epiphanous.flinkrunner.model
import io.epiphanous.flinkrunner.model.SqlColumnTypeConfig.{PREC_SC_NN, PREC_SC_YN, PREC_SC_YY}

import scala.collection.BitSet

case class SqlColumnTypeConfig(
    isSupported: Boolean = true,
    name: Option[String] = None,
    signatures: BitSet = BitSet(PREC_SC_NN),
    maxPrecision: Int = Integer.MAX_VALUE
) {
  val allowsPSNN: Boolean      = signatures.contains(PREC_SC_NN)
  val allowsPSYN: Boolean      = signatures.contains(PREC_SC_YN)
  val allowsPSYY: Boolean      = signatures.contains(PREC_SC_YY)
  val allowsPrecision: Boolean = allowsPSYN || allowsPSYY
  val allowsScale: Boolean     = allowsPSYY
}

object SqlColumnTypeConfig {
  final val PREC_SC_NN: Int = 1
  final val PREC_SC_YN: Int = 2
  final val PREC_SC_YY: Int = 4

  final val PS_NN = BitSet(PREC_SC_NN)
  final val PS_YN = BitSet(PREC_SC_YN)
  final val PS_YY = BitSet(PREC_SC_YY)
}
