package io.epiphanous.flinkrunner.serde

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.model.sink.KafkaSinkConfig
import io.epiphanous.flinkrunner.model.source.KafkaSourceConfig
import io.epiphanous.flinkrunner.{FlinkRunner, PropSpec}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.flink.api.common.functions.util.ListCollector
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.scalatest.Assertion

import java.io.ByteArrayOutputStream
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Success, Try}

trait SerdeTestFixtures extends PropSpec {

  val defaultConfluentSchemaRegConfigStr: String =
    s"""
       |schema.registry {
       |  url = "mock://test"
       |  ${KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG} = true
       |  ${KafkaAvroSerializerConfig.AVRO_REMOVE_JAVA_PROPS_CONFIG} = true
       |}
       |""".stripMargin

  val defaultGlueSchemaRegConfigStr: String =
    s"""
       |schema.registry {
       |  ${AWSSchemaRegistryConstants.AWS_REGION} = "us-east-1"
       |  ${AWSSchemaRegistryConstants.REGISTRY_NAME} = "datafabric"
       |}
       |""".stripMargin

  case class SchemaRegistrySerdeTest[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A >: Null <: GenericRecord: TypeInformation,
      ADT <: FlinkEvent: TypeInformation](
      event: E,
      schemaRegConfigStr: String = defaultConfluentSchemaRegConfigStr
  )(implicit fromKV: EmbeddedAvroRecordInfo[A] => E) {

    val configStr: String                     =
      s"""|
          |jobs {
          |  DeduplicationJob {
          |    sourceNames = [ observations ]
          |    sinkNames = [ test ]
          |  }
          |}
          |sources {
          |  test {
          |    connector = kafka
          |    topic = test
          |    isKeyed = true
          |    bootstrap.servers = "kafka:9092"
          |    $schemaRegConfigStr
          |  }
          |}
          |sinks {
          |  test {
          |    connector = kafka
          |    topic = test
          |    isKeyed = true
          |    bootstrap.servers = "kafka:9092"
          |    $schemaRegConfigStr
          |  }
          |}
          |""".stripMargin
    val runner: FlinkRunner[ADT]              =
      getIdentityAvroStreamJobRunner[E, A, ADT](
        configStr = configStr,
        args = Array("serde-test")
      )
    val kafkaSinkConfig: KafkaSinkConfig[ADT] =
      runner.getSinkConfig("test").asInstanceOf[KafkaSinkConfig[ADT]]

    val schemaRegistryType: SchemaRegistryType =
      kafkaSinkConfig.schemaRegistryConfig.schemaRegistryType
    val isConfluent: Boolean                   =
      schemaRegistryType == SchemaRegistryType.Confluent
    val isGlue: Boolean                        = schemaRegistryType == SchemaRegistryType.AwsGlue

    val kafkaSourceConfig: KafkaSourceConfig[ADT] = runner
      .getSourceConfig("test")
      .asInstanceOf[KafkaSourceConfig[ADT]]

    val avroClass: Class[A] = implicitly[TypeInformation[A]].getTypeClass

    val keySchema: Schema   =
      new Schema.Parser().parse("""{"type":"string"}""")
    val valueSchema: Schema = event.$record.getSchema

    val recordKeyBytes: Array[Byte]   = binaryEncode(event.$id, keySchema)
    val recordValueBytes: Array[Byte] =
      binaryEncode(event.$record, valueSchema)

    val keySubject   = s"${kafkaSourceConfig.topic}-key"
    val valueSubject = s"${kafkaSourceConfig.topic}-value"

    def registerSchemas: Unit = {
      if (isConfluent) {
        def _reg(client: SchemaRegistryClient) = {
          client.register(
            keySubject,
            new AvroSchema(keySchema)
          )
          client.register(
            valueSubject,
            new AvroSchema(valueSchema)
          )
        }
        _reg(kafkaSourceConfig.schemaRegistryConfig.confluentClient)
        _reg(kafkaSinkConfig.schemaRegistryConfig.confluentClient)
      } else { // if (isGlue) {
        val client = kafkaSourceConfig.schemaRegistryConfig.glueClient
        client
          .registerSchemaVersion(keySchema.toString, keySubject, "AVRO")
        client
          .registerSchemaVersion(
            valueSchema.toString,
            valueSubject,
            "AVRO"
          )
      }
    }
    def getConfluentDeserializer
        : ConfluentAvroRegistryKafkaRecordDeserializationSchema[
          E,
          A,
          ADT
        ] = {
      val ds = new ConfluentAvroRegistryKafkaRecordDeserializationSchema[
        E,
        A,
        ADT
      ](kafkaSourceConfig)
      ds.open(null)
      ds
    }

    def getConfluentSerializer
        : ConfluentAvroRegistryKafkaRecordSerializationSchema[
          E,
          A,
          ADT
        ] = {
      val ss =
        new ConfluentAvroRegistryKafkaRecordSerializationSchema[E, A, ADT](
          kafkaSinkConfig
        )
      ss.open(null, null)
      ss
    }

    def getGlueDeserializer
        : GlueAvroRegistryKafkaRecordDeserializationSchema[E, A, ADT] = {
      val ds =
        new GlueAvroRegistryKafkaRecordDeserializationSchema[E, A, ADT](
          kafkaSourceConfig
        )
      ds.open(null)
      ds
    }

    def getGlueSerializer
        : GlueAvroRegistryKafkaRecordSerializationSchema[E, A, ADT] = {
      val ss =
        new GlueAvroRegistryKafkaRecordSerializationSchema[E, A, ADT](
          kafkaSinkConfig
        )
      ss.open(null, null)
      ss
    }

    def binaryEncode[T](obj: T, schema: Schema): Array[Byte] = {
      val baos        = new ByteArrayOutputStream()
      val encoder     = EncoderFactory.get().binaryEncoder(baos, null)
      val datumWriter = new GenericDatumWriter[T](schema)
      datumWriter.write(obj, encoder)
      encoder.flush()
      val bytes       = baos.toByteArray
      baos.close()
      bytes
    }

    def runTest: Assertion = {
      registerSchemas
      val (serializer, deserializer) =
        if (isConfluent) (getConfluentSerializer, getConfluentDeserializer)
        else (getGlueSerializer, getGlueDeserializer)
      val seq                        = new ArrayBuffer[E]().asJava
      val collector                  = new ListCollector[E](seq)
      val out                        = for {
        serializedResult <-
          Try(serializer.serialize(event, null, event.$timestamp))
        (serializedKey, serializedValue) <-
          Success((serializedResult.key(), serializedResult.value()))
        deserializedEvent <- Try(
                               deserializer.deserialize(
                                 new ConsumerRecord(
                                   kafkaSourceConfig.topic,
                                   1,
                                   1,
                                   serializedKey,
                                   serializedValue
                                 ),
                                 collector
                               )
                             ).map(_ => seq.get(0))
      } yield deserializedEvent
      out.success.value shouldEqual event
    }

  }

}
