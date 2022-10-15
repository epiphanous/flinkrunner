package io.epiphanous.flinkrunner.serde

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.sink.KafkaSinkConfig
import io.epiphanous.flinkrunner.model.{EmbeddedAvroRecord, FlinkEvent}
import io.epiphanous.flinkrunner.util.SinkDestinationNameUtils.RichSinkDestinationName
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeaders

import java.lang
import java.nio.charset.StandardCharsets

/** A serialization schema that uses a confluent avro schema registry
  * client to serialize an instance of a flink runner ADT into kafka. The
  * flink runner ADT class must also extend the EmbeddedAvroRecord trait.
  * @param sinkConfig
  *   the kafka sink config
  */
case class ConfluentAvroRegistryKafkaRecordSerializationSchema[
    E <: ADT with EmbeddedAvroRecord[A],
    A <: GenericRecord,
    ADT <: FlinkEvent
](
    sinkConfig: KafkaSinkConfig[ADT]
) extends KafkaRecordSerializationSchema[E]
    with LazyLogging {

  @transient lazy val serializerCacheLoader
      : CacheLoader[Schema, ConfluentRegistryAvroSerializationSchema[
        GenericRecord
      ]] =
    new CacheLoader[Schema, ConfluentRegistryAvroSerializationSchema[
      GenericRecord
    ]] {
      override def load(schema: Schema)
          : ConfluentRegistryAvroSerializationSchema[GenericRecord] =
        ConfluentRegistryAvroSerializationSchema
          .forGeneric(
            s"${schema.getFullName}-value",
            schema,
            sinkConfig.schemaRegistryConfig.url,
            sinkConfig.schemaRegistryConfig.props
          )

    }

  @transient lazy val serializerCache
      : LoadingCache[Schema, ConfluentRegistryAvroSerializationSchema[
        GenericRecord
      ]] = {
    val cacheBuilder = CacheBuilder
      .newBuilder()
      .concurrencyLevel(sinkConfig.cacheConcurrencyLevel)
      .maximumSize(sinkConfig.cacheMaxSize)
      .expireAfterWrite(sinkConfig.cacheExpireAfter)
    if (sinkConfig.cacheRecordStats) cacheBuilder.recordStats()
    cacheBuilder.build[Schema, ConfluentRegistryAvroSerializationSchema[
      GenericRecord
    ]](serializerCacheLoader)
  }

  override def serialize(
      element: E,
      context: KafkaRecordSerializationSchema.KafkaSinkContext,
      timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val info = element.toKV(sinkConfig.config)

    val headers = new RecordHeaders()

    Option(info.headers).foreach { m =>
      m.foreach { case (hk, hv) =>
        headers.add(hk, hv.getBytes(StandardCharsets.UTF_8))
      }
    }

    val topic = sinkConfig.expandTemplate(info.record)

    val key = info.keyOpt.map(_.getBytes(StandardCharsets.UTF_8))
    logger.trace(
      s"serializing ${info.record.getSchema.getFullName} record ${element.$id} to $topic ${if (sinkConfig.isKeyed) "with key"
        else "without key"}, headers=${info.headers}"
    )

    val value =
      serializerCache.get(info.record.getSchema).serialize(info.record)

    new ProducerRecord(
      topic,
      null,
      element.$timestamp,
      key.orNull,
      value,
      headers
    )
  }

}
