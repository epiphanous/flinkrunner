package io.epiphanous.flinkrunner.model
import java.time.Instant

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.aggregate.Aggregate
import squants.energy.{Energy, Power}
import squants.information.{DataRate, Information}
import squants.market.Money
import squants.mass.{AreaDensity, ChemicalAmount, Density, Mass, MomentOfInertia}
import squants.motion._
import squants.photo._
import squants.radio._
import squants.space._
import squants.thermal.{Temperature, ThermalCapacity}
import squants.time.{Frequency, Time}
import squants.{Dimension, Dimensionless, Quantity}

import scala.util.{Failure, Success, Try}

trait UnitMapper extends LazyLogging {

  implicit class TryAggregateOps(tryAggregate: Try[Option[Aggregate]]) {
    def toOptionWithLogging(onFailureMessage: String): Option[Aggregate] = {
      tryAggregate match {
        case Success(updatedOpt) => updatedOpt
        case Failure(t) =>
          logger.error(s"$onFailureMessage\n$t")
          None
      }
    }
  }

  def createQuantity[A <: Quantity[A]](dimension: Dimension[A], value: Double, unit: String): Option[A] =
    dimension.symbolToUnit(unit).map(u => u(value)) match {
      case Some(q: A @unchecked) => Some(q)
      case None =>
        logger.error(s"Can't create quantity $dimension($value, $unit)")
        None
    }

  def updateAggregateWith(aggregate: Aggregate, value: Double, unit: String, aggLU: Instant): Option[Aggregate] = {
    val symbol = getSymbolFromString(aggregate.dimension, unit)
    val vu = (value, unit)
    val vs = (value, symbol)
    val vss = s"$value $symbol"
    val dim = aggregate.dimension
    (dim match {
      case "Acceleration"        => Acceleration(vs).map(q => aggregate.update(q, aggLU, this))
      case "Angle"               => Angle(vs).map(q => aggregate.update(q, aggLU, this))
      case "AngularAcceleration" => AngularAcceleration(vs).map(q => aggregate.update(q, aggLU, this))
      case "AngularVelocity"     => AngularVelocity(vs).map(q => aggregate.update(q, aggLU, this))
      case "Area"                => Area(vs).map(q => aggregate.update(q, aggLU, this))
      case "AreaDensity"         => AreaDensity(vs).map(q => aggregate.update(q, aggLU, this))
      case "ChemicalAmount"      => ChemicalAmount(vs).map(q => aggregate.update(q, aggLU, this))
      case "DataRate"            => DataRate(vs).map(q => aggregate.update(q, aggLU, this))
      case "Density"             => Density(vs).map(q => aggregate.update(q, aggLU, this))
      case "Dimensionless"       => Dimensionless(vs).map(q => aggregate.update(q, aggLU, this))
      case "Energy"              => Energy(vs).map(q => aggregate.update(q, aggLU, this))
      case "Force"               => Force(vs).map(q => aggregate.update(q, aggLU, this))
      case "Frequency"           => Frequency(vs).map(q => aggregate.update(q, aggLU, this))
      case "Illuminance"         => Illuminance(vs).map(q => aggregate.update(q, aggLU, this))
      case "Information"         => Information(vs).map(q => aggregate.update(q, aggLU, this))
      case "Irradiance"          => Irradiance(vs).map(q => aggregate.update(q, aggLU, this))
      case "Jerk"                => Jerk(vs).map(q => aggregate.update(q, aggLU, this))
      case "Length"              => Length(vs).map(q => aggregate.update(q, aggLU, this))
      case "Luminance"           => Luminance(vs).map(q => aggregate.update(q, aggLU, this))
      case "LuminousEnergy"      => LuminousEnergy(vs).map(q => aggregate.update(q, aggLU, this))
      case "LuminousExposure"    => LuminousExposure(vs).map(q => aggregate.update(q, aggLU, this))
      case "LuminousFlux"        => LuminousFlux(vs).map(q => aggregate.update(q, aggLU, this))
      case "LuminousIntensity"   => LuminousIntensity(vs).map(q => aggregate.update(q, aggLU, this))
      case "Mass"                => Mass(vs).map(q => aggregate.update(q, aggLU, this))
      case "MassFlow"            => MassFlow(vs).map(q => aggregate.update(q, aggLU, this))
      case "Momentum"            => Momentum(vs).map(q => aggregate.update(q, aggLU, this))
      case "MomentOfInertia"     => MomentOfInertia(vs).map(q => aggregate.update(q, aggLU, this))
      //needs an implicit MoneyContext...
      //case "Money"               => Money(vss).map(q => aggregate.update(q, aggLU, this))
      case "Power"              => Power(vs).map(q => aggregate.update(q, aggLU, this))
      case "Pressure"           => Pressure(vs).map(q => aggregate.update(q, aggLU, this))
      case "PressureChange"     => PressureChange(vs).map(q => aggregate.update(q, aggLU, this))
      case "Radiance"           => Radiance(vs).map(q => aggregate.update(q, aggLU, this))
      case "RadiantIntensity"   => RadiantIntensity(vs).map(q => aggregate.update(q, aggLU, this))
      case "SolidAngle"         => SolidAngle(vs).map(q => aggregate.update(q, aggLU, this))
      case "SpectralIntensity"  => SpectralIntensity(vs).map(q => aggregate.update(q, aggLU, this))
      case "SpectralIrradiance" => SpectralIrradiance(vs).map(q => aggregate.update(q, aggLU, this))
      case "SpectralPower"      => SpectralPower(vs).map(q => aggregate.update(q, aggLU, this))
      case "Temperature"        => Temperature(vss).map(q => aggregate.update(q, aggLU, this))
      case "ThermalCapacity"    => ThermalCapacity(vs).map(q => aggregate.update(q, aggLU, this))
      case "Time"               => Time(vs).map(q => aggregate.update(q, aggLU, this))
      case "Torque"             => Torque(vs).map(q => aggregate.update(q, aggLU, this))
      case "Velocity"           => Velocity(vs).map(q => aggregate.update(q, aggLU, this))
      case "Volume"             => Volume(vs).map(q => aggregate.update(q, aggLU, this))
      case "VolumeFlow"         => VolumeFlow(vs).map(q => aggregate.update(q, aggLU, this))
      case "Yank"               => Yank(vs).map(q => aggregate.update(q, aggLU, this))
      case _                    => Failure(new UnsupportedOperationException(s"Unsupported Aggregate dimension $dim"))
    }).toOptionWithLogging(s"${aggregate.name}[$dim] could not be updated with $vu")
  }

  def getSymbolFromString(dimension: String, unit: String): String = unit

}

class DefaultUnitMapper extends UnitMapper

object UnitMapper {
  val defaultUnitMapper = new DefaultUnitMapper()
}
