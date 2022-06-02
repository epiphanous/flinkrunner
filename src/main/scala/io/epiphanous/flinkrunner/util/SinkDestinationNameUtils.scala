package io.epiphanous.flinkrunner.util

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.sink._
import org.apache.avro.generic.GenericRecord

/**
 * Utilities to support dynamic sink destination names created from the
 * values being serialized. This logic will expand the following tokens in
 * kafka sink topics, kinesis sink streams, file sink paths and elastic
 * sink indexes:
 *
 *   - <tt>{canonical-name}</tt>: for avro containers, expands to the full
 *     name of the schema; for other values expands to the result of
 *     <tt>value.getClass.getCanonicalName</tt>
 *   - <tt>{simple-name}<tt>: for avro containers, expands to the short
 *     name of the schema; for other values, expands to the result of
 *     <tt>value.getClass.getSimpleName</tt>
 *
 * Null values always expand all tokens to <tt>"null"</tt>.
 */
object SinkDestinationNameUtils {
  def expandDestinationTemplate[T](template: String, value: T): String =
    dataFromValue(value)
      .foldLeft(template) { case (t, (k, v)) =>
        t.replaceAll(s"\\{$k}", v)
      }

  def dataFromValue[T](value: T): Map[String, String] = value match {
    case v if Option(v).isEmpty   =>
      Map(
        "canonical-name" -> "null",
        "simple-name"    -> "null"
      )
    case container: GenericRecord =>
      val schema = container.getSchema
      Map(
        "canonical-name" -> schema.getFullName,
        "simple-name"    -> schema.getName
      )
    case _                        =>
      val klass = value.getClass
      Map(
        "canonical-name" -> klass.getCanonicalName,
        "simple-name"    -> klass.getSimpleName
      )
  }

  implicit class RichSinkDestinationName(sinkConfig: SinkConfig)
      extends LazyLogging {
    def expandTemplate[T](value: T): String = {
      val template = sinkConfig match {
        // normalize kafka topics
        case s: KafkaSinkConfig         =>
          val normalized = s.topic.toLowerCase
            .replaceAll("[^a-z\\d._\\-]", "")
            // standardize on periods instead of underscores
            .replaceAll(
              "_",
              "."
            )
          if (!s.topic.equals(normalized))
            logger.warn(
              s"""kafka topic name template "${s.topic}" normalized to "$normalized""""
            )
          normalized
        // TODO: get rules to normalize these names too
        case s: KinesisSinkConfig       => s.stream
        case s: FileSinkConfig          => s.path
        case s: ElasticsearchSinkConfig => s.index
        case s                          => s.name
      }
      // only expand templates if they contain template characters
      if (template.contains("{"))
        expandDestinationTemplate(template, value)
      else template
    }
  }
}
