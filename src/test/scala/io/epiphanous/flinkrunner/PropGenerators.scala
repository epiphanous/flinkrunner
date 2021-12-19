package io.epiphanous.flinkrunner

import io.epiphanous.flinkrunner.model.{
  ARecord,
  AWrapper,
  BRecord,
  BWrapper,
  MyAvroADT
}
import org.scalacheck.{Arbitrary, Gen}

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.util.Random

trait PropGenerators {

  def idGen(size: Int = 7): Gen[String] = {
    for {
      id <- Gen.identifier.suchThat(s => s.length >= size)
    } yield id.take(size)
  }
  implicit lazy val idArb: Arbitrary[String] = Arbitrary(idGen())

  def instantGen(
      back: Duration = Duration.create(90, TimeUnit.DAYS),
      forward: Duration = Duration.Zero,
      fromInstant: Instant = Instant.now()): Gen[Instant] = {
    val start = fromInstant.minusMillis(back.toMillis).toEpochMilli
    val end   = fromInstant.plusMillis(forward.toMillis).toEpochMilli
    Gen.choose(start, end).map(Instant.ofEpochMilli)
  }
  implicit lazy val instantArb: Arbitrary[Instant] = Arbitrary(
    instantGen()
  )

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

  def uuidGen: Gen[String]                     = Gen.uuid.map(u => u.toString)
  implicit lazy val uuidArb: Arbitrary[String] = Arbitrary(uuidGen)

  def genPopWith[T](mean: Int, sd: Double, arb: Arbitrary[T]): List[T] =
    genPop[T](mean, sd)(arb)

  def genOneWith[T](arb: Arbitrary[T]): T =
    genOne[T](arb)

  def genOne[T](implicit arb: Arbitrary[T]): T = genPop[T](1).head

  def genPop[T](
      mean: Int,
      sd: Double = 0
  )(implicit arb: Arbitrary[T]): List[T] =
    Stream
      .from(0)
      .map(_ => arb.arbitrary.sample)
      .filter(_.nonEmpty)
      .take(((Random.nextGaussian() - 0.5) * sd + mean).round.toInt)
      .flatten
      .toList
}
