package io.epiphanous.flinkrunner.util

import com.codahale.metrics.ExponentiallyDecayingReservoir
import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper
import org.apache.flink.metrics._

object MetricUtils {

  def unregisteredMetricMessage(
      metricType: String,
      key: String,
      value: String): String =
    s"failed to update unregistered $metricType metric (key=$key,value=$value)"

  implicit class RichVariableCounters(
      counters: Map[String, Map[String, Counter]])
      extends LazyLogging {
    def inc(key: String, value: String, num: Long = 1): Unit =
      if (counters.contains(key) && counters(key).contains(value))
        counters(key)(value).inc(num)
      else logger.warn(unregisteredMetricMessage("counter", key, value))

    def dec(key: String, value: String, num: Long = 1): Unit =
      if (counters.contains(key) && counters(key).contains(value))
        counters(key)(value).dec(num)
      else logger.warn(unregisteredMetricMessage("counter", key, value))
  }

  implicit class RichVariableMeters(
      meters: Map[String, Map[String, Meter]])
      extends LazyLogging {
    def markEvent(key: String, value: String, num: Long = 1): Unit =
      if (meters.contains(key) && meters(key).contains(value))
        meters(key)(value).markEvent(num)
      else logger.warn(unregisteredMetricMessage("meter", key, value))
  }

  implicit class RichVariableHistograms(
      histograms: Map[String, Map[String, DropwizardHistogramWrapper]])
      extends LazyLogging {
    def update(key: String, value: String, updatedValue: Long): Unit =
      if (histograms.contains(key) && histograms(key).contains(value))
        histograms(key)(value).update(updatedValue)
      else
        logger.warn(unregisteredMetricMessage("histogram", key, value))
  }

  def defaultHistogramFactory() = new com.codahale.metrics.Histogram(
    new ExponentiallyDecayingReservoir()
  )

  implicit class RichMetricGroup(mg: MetricGroup) {

    def registerMetricByVariables[M](
        name: String,
        variables: Map[String, List[String]],
        func: MetricGroup => M): Map[String, Map[String, M]] =
      variables.map { case (key, values) =>
        key -> values
          .map(value => value -> func(mg.addGroup(key, value)))
          .toMap
      }

    def registerCounter(counterName: String): Counter =
      mg.counter(counterName)

    def registerCounterByVariables(
        counterName: String,
        variables: Map[String, List[String]])
        : Map[String, Map[String, Counter]] =
      registerMetricByVariables(
        counterName,
        variables,
        _.registerCounter(counterName)
      )

    /** Registers a meter and an associated counter in this metric group.
      * The name of the meter is derived from the provided counter name.
      *
      * @param counterName
      *   name of the counter
      * @param span
      *   the number of seconds over which counts are aggregated to compute
      *   the rate (defaults to 60 seconds)
      * @return
      *   Meter
      */
    def registerMeter(counterName: String, span: Int = 60): Meter = {
      val rateName = s"$counterName-per-second"
      mg.meter(
        rateName,
        new MeterView(mg.counter(counterName), span)
      )
    }

    /** Register a set of meters, one for each pair of label and associated
      * values provided in the labels parameter.
      * @param counterName
      *   name of the counter within the meter
      * @param variables
      *   map of labels, with a list of values for each
      * @param span
      *   number of seconds over which counts are aggregated to compute the
      *   rate in the meters (defaults to 60 seconds)
      * @return
      *   Map[String, Map[String, Meter`]``]`
      */
    def registerMeterByVariables(
        counterName: String,
        variables: Map[String, List[String]],
        span: Int = 60): Map[String, Map[String, Meter]] =
      registerMetricByVariables(
        counterName,
        variables,
        _.registerMeter(counterName, span)
      )

    def registerGauge[T](name: String, gauge: Gauge[T]): Gauge[T] =
      mg.gauge[T, Gauge[T]](name, gauge)

    def registerGaugeByVariables[T](
        name: String,
        gauge: Gauge[T],
        variables: Map[String, List[String]])
        : Map[String, Map[String, Gauge[T]]] =
      registerMetricByVariables(
        name,
        variables,
        _.registerGauge[T](name, gauge)
      )

    def registerHistogram(
        name: String,
        histogram: com.codahale.metrics.Histogram =
          defaultHistogramFactory()): DropwizardHistogramWrapper =
      mg.histogram(name, new DropwizardHistogramWrapper(histogram))

    def registerHistogramByVariables(
        name: String,
        variables: Map[String, List[String]],
        histogramFactory: () => com.codahale.metrics.Histogram =
          defaultHistogramFactory)
        : Map[String, Map[String, DropwizardHistogramWrapper]] =
      registerMetricByVariables(
        name,
        variables,
        _.registerHistogram(name, histogramFactory())
      )
  }

}
