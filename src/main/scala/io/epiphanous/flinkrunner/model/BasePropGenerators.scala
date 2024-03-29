package io.epiphanous.flinkrunner.model

import org.scalacheck.{Arbitrary, Gen}

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.Random

trait BasePropGenerators {
  def idGen(size: Int = 7): Gen[String]      = {
    for {
      id <- Gen.identifier.suchThat(s => s.length >= size)
    } yield id.take(size)
  }
  implicit lazy val idArb: Arbitrary[String] = Arbitrary(idGen())

  def nameGen(kind: String, size: Int = 4): Gen[String] =
    for {
      alpha <- Gen.alphaUpperStr.suchThat(s => s.length >= size)
      num <- Gen.numStr.suchThat(s => s.length >= size)
    } yield List(kind, alpha.take(size).toUpperCase(), num.take(size))
      .mkString("-")
  implicit lazy val nameArb: Arbitrary[String]          = Arbitrary(nameGen("Arb"))

  def instantGen(
      back: Duration = Duration.create(60, TimeUnit.SECONDS),
      forward: Duration = Duration.Zero,
      fromInstant: Instant = Instant.now()): Gen[Instant] = {
    val start = fromInstant.minusMillis(back.toMillis).toEpochMilli
    val end   = fromInstant.plusMillis(forward.toMillis).toEpochMilli
    Gen.choose(start, end).map(Instant.ofEpochMilli)
  }
  implicit lazy val instantArb: Arbitrary[Instant]         = Arbitrary(
    instantGen()
  )

  def durationGen(
      min: Long = 1,
      max: Long = 90,
      units: TimeUnit = TimeUnit.DAYS): Gen[FiniteDuration] =
    for {
      num <- Gen.choose(min, max)
      duration <- Gen.const(Duration.create(num, units))
    } yield duration
  implicit lazy val durationArb: Arbitrary[FiniteDuration] = Arbitrary(
    durationGen()
  )

  def uuidGen: Gen[String]                     = Gen.uuid.map(u => u.toString)
  implicit lazy val uuidArb: Arbitrary[String] = Arbitrary(uuidGen)

  def genPopWith[T](mean: Int, sd: Double, arb: Arbitrary[T]): List[T] =
    genPop[T](mean, sd)(arb)

  def genOneWith[T](arb: Arbitrary[T]): T =
    genOne[T](arb)

  def genStreamWith[T](arb: Arbitrary[T]): Stream[T] = genStream(arb)

  def genOne[T](implicit arb: Arbitrary[T]): T = genPop[T](1).head

  def genStream[T](implicit arb: Arbitrary[T]): Stream[T] =
    Stream
      .from(0)
      .flatMap(_ => arb.arbitrary.sample)

  def genPop[T](
      mean: Int = 10,
      sd: Double = 0
  )(implicit arb: Arbitrary[T]): List[T] =
    genStream[T]
      .take(((Random.nextGaussian() - 0.5) * sd + mean).round.toInt)
      .toList
}
