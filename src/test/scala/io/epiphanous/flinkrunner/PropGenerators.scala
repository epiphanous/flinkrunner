package io.epiphanous.flinkrunner

import io.epiphanous.flinkrunner.model._
import org.scalacheck.{Arbitrary, Gen}

trait PropGenerators extends BasePropGenerators {

  lazy val aRecordGen: Gen[ARecord]                = for {
    a0 <- idGen()
    a1 <- Gen.chooseNum(0, 1000)
    a2 <- Gen.chooseNum(1d, 100d)
    a3 <- instantGen()
  } yield ARecord(a0, a1, a2, a3)
  implicit lazy val aRecordArb: Arbitrary[ARecord] = Arbitrary(aRecordGen)

  lazy val bRecordGen: Gen[BRecord]                = for {
    b0 <- idGen()
    b1 <- Gen.option(Gen.chooseNum(0, 1000))
    b2 <- Gen.option(Gen.chooseNum(1d, 100d))
    b3 <- instantGen()
  } yield BRecord(b0, b1, b2, b3)
  implicit lazy val bRecordArb: Arbitrary[BRecord] = Arbitrary(bRecordGen)

  lazy val aWrapperGen: Gen[AWrapper]                = for {
    a <- aRecordGen
  } yield AWrapper(a)
  implicit lazy val aWrapperArb: Arbitrary[AWrapper] = Arbitrary(
    aWrapperGen
  )

  lazy val bWrapperGen: Gen[BWrapper]                = for {
    b <- bRecordGen
  } yield BWrapper(b)
  implicit lazy val bWrapperArb: Arbitrary[BWrapper] = Arbitrary(
    bWrapperGen
  )

  lazy val myAvroADTGen: Gen[MyAvroADT]                =
    Gen.oneOf(aWrapperGen, bWrapperGen)
  implicit lazy val myAvroADTArb: Arbitrary[MyAvroADT] = Arbitrary(
    myAvroADTGen
  )

  lazy val simpleAGen: Gen[SimpleA]                = for {
    id <- idGen()
    a0 <- nameGen("A")
    a1 <- Gen.chooseNum(100, 199)
    ts <- instantGen()
  } yield SimpleA(id, a0, a1, ts)
  implicit lazy val simpleAArb: Arbitrary[SimpleA] = Arbitrary(simpleAGen)

  lazy val simpleBGen: Gen[SimpleB]                = for {
    id <- idGen()
    b0 <- nameGen("B")
    b1 <- Gen.chooseNum(200d, 299d)
    b2 <- Gen.option(Gen.chooseNum(200, 299))
    ts <- instantGen()
  } yield SimpleB(id, b0, b1, b2, ts)
  implicit lazy val simpleBArb: Arbitrary[SimpleB] = Arbitrary(simpleBGen)

  lazy val simpleCGen: Gen[SimpleC]                = for {
    id <- idGen()
    c1 <- nameGen("C")
    c2 <- Gen.chooseNum(300d, 399d)
    c3 <- Gen.chooseNum(300, 399)
    ts <- instantGen()
  } yield SimpleC(id, c1, c2, c3, ts)
  implicit lazy val simpleCArb: Arbitrary[SimpleC] = Arbitrary(simpleCGen)

  lazy val mySimpleADTGen: Gen[MySimpleADT]                =
    Gen.oneOf(simpleAGen, simpleBGen, simpleCGen)
  implicit lazy val mySimpleADTArb: Arbitrary[MySimpleADT] = Arbitrary(
    mySimpleADTGen
  )

}
