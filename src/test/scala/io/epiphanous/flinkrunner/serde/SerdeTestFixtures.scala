package io.epiphanous.flinkrunner.serde

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.dimafeng.testcontainers.GenericContainer
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import io.epiphanous.flinkrunner.model._
import io.epiphanous.flinkrunner.model.sink.KafkaSinkConfig
import io.epiphanous.flinkrunner.model.source.KafkaSourceConfig
import io.epiphanous.flinkrunner.util.AvroUtils.schemaOf
import io.epiphanous.flinkrunner.{FlinkRunner, PropSpec}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.functions.util.ListCollector
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.scalatest.Assertion
import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials,
  DefaultCredentialsProvider,
  StaticCredentialsProvider
}
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.glue.GlueClient
import software.amazon.awssdk.services.glue.model.{
  CreateRegistryRequest,
  CreateSchemaRequest,
  RegistryId
}

import java.net.URI
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

  val gluePort         = 4566
  val glueRegistryName = "test"

  val defaultGlueSchemaRegConfigStr: String =
    s"""
       |schema.registry {
       |  ${AWSSchemaRegistryConstants.AWS_ENDPOINT} = "http://localhost:$gluePort"
       |  ${AWSSchemaRegistryConstants.AWS_REGION} = "us-east-1"
       |  ${AWSSchemaRegistryConstants.REGISTRY_NAME} = "$glueRegistryName"
       |  ${AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING} = true
       |  ${AWSSchemaRegistryConstants.DATA_FORMAT} = avro
       |  ${AWSSchemaRegistryConstants.AVRO_RECORD_TYPE} = SPECIFIC_RECORD
       |}
       |""".stripMargin

  case class SchemaRegistrySerdeTest[
      E <: ADT with EmbeddedAvroRecord[A]: TypeInformation,
      A >: Null <: GenericRecord: TypeInformation,
      ADT <: FlinkEvent: TypeInformation](
      schemaRegConfigStr: String = defaultConfluentSchemaRegConfigStr,
      localstack: Option[GenericContainer] = None
  )(implicit fromKV: EmbeddedAvroRecordInfo[A] => E) {

    val mappedGluePort: Int = localstack
      .map(c => c.container.getMappedPort(gluePort).intValue())
      .getOrElse(gluePort)

    val srcs: String =
      schemaRegConfigStr.replaceFirst(
        "localhost:" + gluePort,
        "localhost:" + mappedGluePort
      )

//    println("Schema Registry Configs:\n" + srcs)

    val avroClass: Class[A] = implicitly[TypeInformation[A]].getTypeClass

    val avroClassName: String = avroClass.getSimpleName
    val topic: String         = avroClassName.toLowerCase

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
          |    topic = "$topic"
          |    isKeyed = true
          |    bootstrap.servers = "kafka:9092"
          |    $srcs
          |  }
          |}
          |sinks {
          |  test {
          |    connector = kafka
          |    topic = "$topic"
          |    isKeyed = true
          |    bootstrap.servers = "kafka:9092"
          |    $srcs
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

    def init(): Assertion = {
      val x = Try {
        if (isGlue) {
          val props            = kafkaSourceConfig.schemaRegistryConfig.props
          val glue: GlueClient = {
            val accessKeyId         = props.getOrDefault("accessKeyId", "foobar")
            val secretAccessKey     =
              props.getOrDefault("secretAccessKey", "foobar")
            val credentialsProvider =
              if (accessKeyId.nonEmpty && secretAccessKey.nonEmpty)
                StaticCredentialsProvider.create(
                  AwsBasicCredentials.create(accessKeyId, secretAccessKey)
                )
              else DefaultCredentialsProvider.builder().build()
            val glueConfig          = new GlueSchemaRegistryConfiguration(props)
            GlueClient
              .builder()
              .credentialsProvider(credentialsProvider)
              .httpClient(UrlConnectionHttpClient.builder.build)
              .region(Region.of(glueConfig.getRegion))
              .endpointOverride(new URI(glueConfig.getEndPoint))
              .build()
          }
          glue.createRegistry(
            CreateRegistryRequest
              .builder()
              .registryName(glueRegistryName)
              .build()
          )
          glue.createSchema(
            CreateSchemaRequest
              .builder()
              .registryId(
                RegistryId
                  .builder()
                  .registryName(glueRegistryName)
                  .build()
              )
              .schemaName(avroClassName.toLowerCase)
              .dataFormat("AVRO")
              .schemaDefinition(schemaOf[A](avroClass).toString)
              .build()
          )
        }
      }
      x.fold(t => t.printStackTrace(), identity)
      x should be a 'Success
    }

    val confluentSchemaRegistryClient = new MockSchemaRegistryClient()

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
      ](
        kafkaSourceConfig,
        Some(confluentSchemaRegistryClient)
      )
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
          kafkaSinkConfig,
          Some(confluentSchemaRegistryClient)
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

    def runTest(event: E): Assertion = {
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
        deserializedEvent <-
          Try(
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
      out.fold(fa => fa.printStackTrace(), identity)
      out.success.value shouldEqual event
    }

  }

}
