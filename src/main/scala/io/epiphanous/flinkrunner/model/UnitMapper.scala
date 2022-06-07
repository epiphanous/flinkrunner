package io.epiphanous.flinkrunner.model

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.aggregate.Aggregate
import squants.energy.{Energy, Power}
import squants.information.{DataRate, Information}
import squants.market.{Money, MoneyContext, USD}
import squants.mass._
import squants.motion._
import squants.photo._
import squants.radio._
import squants.space._
import squants.thermal.{Temperature, ThermalCapacity}
import squants.time.{Frequency, Time}
import squants.{Dimension, Dimensionless, Quantity}

import java.time.Instant
import scala.util.{Failure, Try}

trait UnitMapper extends LazyLogging {

  def getMoneyContext: MoneyContext

  def createQuantity[A <: Quantity[A]](
      dimension: Dimension[A],
      value: Double,
      unit: String): Try[A] =
    Try(
      dimension
        .symbolToUnit(unit)
        .map(u => u(value))
        .getOrElse(
          throw new RuntimeException(
            s"Can't create quantity $dimension($value, $unit)"
          )
        )
    )

  def updateAggregateWith(
      aggregate: Aggregate,
      value: Double,
      unit: String,
      aggLU: Instant): Try[Aggregate] = {
    val symbol = getSymbolFromString(aggregate.dimension, unit)
    val vs     = (value, symbol)
    val vss    = s"$value $symbol"
    val dim    = aggregate.dimension
    dim match {
      case "Acceleration"        =>
        Acceleration(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "Angle"               =>
        Angle(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "AngularAcceleration" =>
        AngularAcceleration(vs).flatMap(q =>
          aggregate.update(q, aggLU, this)
        )
      case "AngularVelocity"     =>
        AngularVelocity(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "Area"                =>
        Area(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "AreaDensity"         =>
        AreaDensity(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "ChemicalAmount"      =>
        ChemicalAmount(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "DataRate"            =>
        DataRate(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "Density"             =>
        Density(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "Dimensionless"       =>
        Dimensionless(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "Energy"              =>
        Energy(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "Force"               =>
        Force(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "Frequency"           =>
        Frequency(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "Illuminance"         =>
        Illuminance(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "Information"         =>
        Information(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "Irradiance"          =>
        Irradiance(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "Jerk"                =>
        Jerk(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "Length"              =>
        Length(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "Luminance"           =>
        Luminance(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "LuminousEnergy"      =>
        LuminousEnergy(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "LuminousExposure"    =>
        LuminousExposure(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "LuminousFlux"        =>
        LuminousFlux(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "LuminousIntensity"   =>
        LuminousIntensity(vs).flatMap(q =>
          aggregate.update(q, aggLU, this)
        )
      case "Mass"                =>
        Mass(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "MassFlow"            =>
        MassFlow(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "Momentum"            =>
        Momentum(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "MomentOfInertia"     =>
        MomentOfInertia(vs).flatMap(q => aggregate.update(q, aggLU, this))
      // needs an implicit MoneyContext...
      case "Money"               =>
        implicit val moneyContext: MoneyContext = getMoneyContext
        Money(vss).flatMap(q => aggregate.update(q, aggLU, this))
      case "Power"               =>
        Power(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "Pressure"            =>
        Pressure(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "PressureChange"      =>
        PressureChange(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "Radiance"            =>
        Radiance(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "RadiantIntensity"    =>
        RadiantIntensity(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "SolidAngle"          =>
        SolidAngle(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "SpectralIntensity"   =>
        SpectralIntensity(vs).flatMap(q =>
          aggregate.update(q, aggLU, this)
        )
      case "SpectralIrradiance"  =>
        SpectralIrradiance(vs).flatMap(q =>
          aggregate.update(q, aggLU, this)
        )
      case "SpectralPower"       =>
        SpectralPower(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "Temperature"         =>
        Temperature(vss).flatMap(q => aggregate.update(q, aggLU, this))
      case "ThermalCapacity"     =>
        ThermalCapacity(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "Time"                =>
        Time(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "Torque"              =>
        Torque(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "Velocity"            =>
        Velocity(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "Volume"              =>
        Volume(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "VolumeFlow"          =>
        VolumeFlow(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case "Yank"                =>
        Yank(vs).flatMap(q => aggregate.update(q, aggLU, this))
      case _                     =>
        Failure(
          new UnsupportedOperationException(
            s"Unsupported Aggregate dimension $dim"
          )
        )
    }
  }

  def getSymbolFromString(dimension: String, unit: String): String =
    Seq(
      unit,
      dimension
    ).head // to avoid stupid warning on unused symbol dimension

}

class DefaultUnitMapper extends UnitMapper {
  override val getMoneyContext: MoneyContext =
    MoneyContext(USD, Set.empty, Seq.empty)
}

object UnitMapper {
  val defaultUnitMapper = new DefaultUnitMapper()
}
