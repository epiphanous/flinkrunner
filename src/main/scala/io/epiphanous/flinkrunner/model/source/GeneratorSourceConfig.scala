package io.epiphanous.flinkrunner.model.source

import io.epiphanous.flinkrunner.model._
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.{Source, SourceSplit}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource

import java.time.Duration

/** A source configuration for a Flink DataGeneratorSource function.
  *
  * This class is abstract because every generator is different. You must
  * implement a concrete subclass of this source config in order to use
  * this in your flink jobs.
  *
  * Configuration options:
  *   - `rows.per.second` - optional long defining number of rows per
  *     second to create (defaults to Long.MaxValue)
  *   - `max.rows` - optional max number of rows to produce (defaults to no
  *     max)
  *   - `seed` - optional random number generator seed (defaults to
  *     built-in java RNG value)
  *   - `start.ago` - optional duration from the current time into the past
  *     to start event timestamps (defaults to 1000 days ago)
  *   - `max.time.step.millis` - optional maximum number of millis to
  *     increment time by for each event (defaults to 100ms)
  *   - `prob.out.of.order` - optional probability of time moving backward
  *     (defaults to 0)
  *   - `prob.null` - optional probability of producing a null value
  *     (defaults to 0)
  *
  * @param name
  *   name of the source
  * @param config
  *   a flinkrunner config
  * @param generatorFactory
  *   a generator factory for creating generators for ADT events
  * @tparam ADT
  *   Flinkrunner algebraic data type
  */
case class GeneratorSourceConfig[ADT <: FlinkEvent](
    name: String,
    config: FlinkConfig,
    generatorFactory: GeneratorFactory[ADT])
    extends SourceConfig[ADT] {

  override def connector: FlinkConnectorName = FlinkConnectorName.Generator

  val generatorConfig: GeneratorConfig = GeneratorConfig(
    name,
    config.getLongOpt(pfx("rows.per.second")).getOrElse(Long.MaxValue),
    config.getLongOpt(pfx("max.rows")).getOrElse(-1),
    config.getLongOpt(pfx("seed")),
    config
      .getDurationOpt(pfx("start.ago"))
      .getOrElse(Duration.ofDays(365)),
    config.getIntOpt(pfx("max.time.step.millis")).getOrElse(100),
    config.getDoubleOpt(pfx("prob.out.of.order")).getOrElse(0),
    config.getDoubleOpt(pfx("prob.null")).getOrElse(0),
    properties
  )

  val isBounded: Boolean = generatorConfig.maxRows > 0

  override def getSource[E <: ADT: TypeInformation]
      : Either[SourceFunction[E], Source[E, _ <: SourceSplit, _]] =
    Left(
      new DataGeneratorSource(
        generatorFactory.getDataGenerator[E](generatorConfig),
        generatorConfig.rowsPerSecond,
        if (generatorConfig.maxRows > 0) generatorConfig.maxRows else null
      )
    )

  override def getAvroSource[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](implicit
      fromKV: EmbeddedAvroRecordInfo[A] => E)
      : Either[SourceFunction[E], Source[E, _ <: SourceSplit, _]] =
    Left(
      new DataGeneratorSource(
        generatorFactory.getAvroDataGenerator[E, A](generatorConfig),
        generatorConfig.rowsPerSecond,
        if (generatorConfig.maxRows > 0) generatorConfig.maxRows else null
      )
    )
}
