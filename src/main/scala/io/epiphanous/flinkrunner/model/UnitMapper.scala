package io.epiphanous.flinkrunner.model
import java.time.Instant

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.aggregate.Aggregate
import squants.Dimensionless
import squants.energy.{Energy, Power}
import squants.information.Information
import squants.market.Money
import squants.mass.{AreaDensity, ChemicalAmount, Density, Mass}
import squants.motion._
import squants.photo._
import squants.radio._
import squants.space._
import squants.thermal.{Temperature, ThermalCapacity}
import squants.time.{Frequency, Time}

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

  def updateAggregateWith(aggregate: Aggregate, value: Double, unit: String, aggLU: Instant): Option[Aggregate] = {
    val symbol = getSymbolFromString(aggregate.dimension, unit)
    val vu = (value, unit)
    val vs = (value, symbol)
    val vss = s"$value $symbol"
    val dim = aggregate.dimension
    (dim match {
      case "Acceleration"      => Acceleration(vs).map(q => aggregate.update(q, aggLU))
      case "Angle"             => Angle(vs).map(q => aggregate.update(q, aggLU))
      case "AngularVelocity"   => AngularVelocity(vs).map(q => aggregate.update(q, aggLU))
      case "Area"              => Area(vs).map(q => aggregate.update(q, aggLU))
      case "AreaDensity"       => AreaDensity(vs).map(q => aggregate.update(q, aggLU))
      case "ChemicalAmount"    => ChemicalAmount(vs).map(q => aggregate.update(q, aggLU))
      case "Density"           => Density(vs).map(q => aggregate.update(q, aggLU))
      case "Dimensionless"     => Dimensionless(vs).map(q => aggregate.update(q, aggLU))
      case "Energy"            => Energy(vs).map(q => aggregate.update(q, aggLU))
      case "Force"             => Force(vs).map(q => aggregate.update(q, aggLU))
      case "Frequency"         => Frequency(vs).map(q => aggregate.update(q, aggLU))
      case "Illuminance"       => Illuminance(vs).map(q => aggregate.update(q, aggLU))
      case "Information"       => Information(vs).map(q => aggregate.update(q, aggLU))
      case "Irradiance"        => Irradiance(vs).map(q => aggregate.update(q, aggLU))
      case "Jerk"              => Jerk(vs).map(q => aggregate.update(q, aggLU))
      case "Length"            => Length(vs).map(q => aggregate.update(q, aggLU))
      case "Luminance"         => Luminance(vs).map(q => aggregate.update(q, aggLU))
      case "LuminousEnergy"    => LuminousEnergy(vs).map(q => aggregate.update(q, aggLU))
      case "LuminousExposure"  => LuminousExposure(vs).map(q => aggregate.update(q, aggLU))
      case "LuminousFlux"      => LuminousFlux(vs).map(q => aggregate.update(q, aggLU))
      case "LuminousIntensity" => LuminousIntensity(vs).map(q => aggregate.update(q, aggLU))
      case "Mass"              => Mass(vs).map(q => aggregate.update(q, aggLU))
      case "MassFlow"          => MassFlow(vs).map(q => aggregate.update(q, aggLU))
      case "Momentum"          => Momentum(vs).map(q => aggregate.update(q, aggLU))
      case "Money"             => Money(vss).map(q => aggregate.update(q, aggLU))
      case "Power"             => Power(vs).map(q => aggregate.update(q, aggLU))
      case "Pressure"          => Pressure(vs).map(q => aggregate.update(q, aggLU))
      case "Radiance"          => Radiance(vs).map(q => aggregate.update(q, aggLU))
      case "RadiantIntensity"  => RadiantIntensity(vs).map(q => aggregate.update(q, aggLU))
      case "SolidAngle"        => SolidAngle(vs).map(q => aggregate.update(q, aggLU))
      case "SpectralIntensity" => SpectralIntensity(vs).map(q => aggregate.update(q, aggLU))
      case "SpectralPower"     => SpectralPower(vs).map(q => aggregate.update(q, aggLU))
      case "Temperature"       => Temperature(vss).map(q => aggregate.update(q, aggLU))
      case "ThermalCapacity"   => ThermalCapacity(vs).map(q => aggregate.update(q, aggLU))
      case "Time"              => Time(vs).map(q => aggregate.update(q, aggLU))
      case "Velocity"          => Velocity(vs).map(q => aggregate.update(q, aggLU))
      case "Volume"            => Volume(vs).map(q => aggregate.update(q, aggLU))
      case "VolumeFlow"        => VolumeFlow(vs).map(q => aggregate.update(q, aggLU))
      case "Yank"              => Yank(vs).map(q => aggregate.update(q, aggLU))
      case _                   => Failure(new UnsupportedOperationException(s"Unsupported Aggregate dimension $dim"))
    }).toOptionWithLogging(s"${aggregate.name}[$dim] could not be updated with $vu")
  }

  def getSymbolFromString(dimension: String, unit: String): String = unit

}

class DefaultUnitMapper extends UnitMapper

object UnitMapper {
  val defaultUnitMapper = new DefaultUnitMapper()
}
