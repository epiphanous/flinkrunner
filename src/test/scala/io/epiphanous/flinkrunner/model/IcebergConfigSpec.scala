package io.epiphanous.flinkrunner.model

import com.dimafeng.testcontainers.lifecycle.and
import com.dimafeng.testcontainers.scalatest.TestContainersForAll
import com.dimafeng.testcontainers.{GenericContainer, LocalStackV2Container}
import io.epiphanous.flinkrunner.PropSpec
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.table.types.logical.RowType
import org.testcontainers.containers.Network
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import org.testcontainers.utility.Base58
import requests.Response
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CreateBucketRequest

import scala.reflect.runtime.{universe => ru}

class IcebergConfigSpec extends PropSpec with TestContainersForAll {
  val bucketName      = "warehouse"
  val icebergRESTPort = 8181
  val icebergHost     = s"iceberg-${Base58.randomString(6)}"
  val localstackHost  = s"localstack-${Base58.randomString(6)}"

  val icebergImage: GenericContainer.DockerImage =
    GenericContainer.stringToDockerImage("tabulario/iceberg-rest:0.2.0")

  override type Containers = LocalStackV2Container and GenericContainer

  override def startContainers(): Containers = {
    val network    = Network.newNetwork()
    val localstack = LocalStackV2Container(
      tag = "1.4.0",
      services = Seq(Service.S3)
    ).configure(_.withNetwork(network).withNetworkAliases(localstackHost))
    localstack.start()

    val creds          = localstack.staticCredentialsProvider.resolveCredentials()
    val env            = Map(
      "CATALOG_S3_ACCESS__KEY__ID"     -> creds.accessKeyId(),
      "CATALOG_S3_SECRET__ACCESS__KEY" -> creds.secretAccessKey(),
      "AWS_REGION"                     -> localstack.region.toString,
      "CATALOG_WAREHOUSE"              -> s"s3://$bucketName/",
      "CATALOG_IO__IMPL"               -> "org.apache.iceberg.aws.s3.S3FileIO",
      "CATALOG_S3_ENDPOINT"            -> s3Endpoint(localstack, outside = false),
      "CATALOG_S3_PATH__STYLE__ACCESS" -> "true"
    )
    logger.debug(env.toString)
    val icebergCatalog = GenericContainer(
      dockerImage = icebergImage,
      exposedPorts = Seq(icebergRESTPort),
      env = env
    ).configure { c =>
      c.withNetwork(network)
      c.withNetworkAliases(icebergHost)
    }
    icebergCatalog.start()
    localstack and icebergCatalog
  }

  override def afterContainersStart(containers: Containers): Unit = {
    super.afterContainersStart(containers)
    containers match {
      case ls and _ =>
        val s3 = getS3Client(ls)
        s3.createBucket(
          CreateBucketRequest.builder().bucket(bucketName).build()
        )
    }
  }

  def getS3Client(ls: LocalStackV2Container): S3Client = {
    S3Client
      .builder()
      .region(ls.region)
      .endpointOverride(ls.endpointOverride(Service.S3))
      .credentialsProvider(ls.staticCredentialsProvider)
      .forcePathStyle(true)
      .build()
  }

  def s3Endpoint(
      ls: LocalStackV2Container,
      outside: Boolean = true): String = {
    if (outside) ls.endpointOverride(Service.S3).toString
    else
      s"http://$localstackHost:4566"
  }

  def icebergEndpoint(
      ib: GenericContainer,
      outside: Boolean = true): String = {
    val mappedPort = ib.container.getMappedPort(icebergRESTPort)
    if (outside) s"http://localhost:$mappedPort"
    else s"http://$icebergHost:$mappedPort"
  }

  def icebergRest(
      ib: GenericContainer,
      path: String,
      params: Iterable[(String, String)] = Nil): Response = {
    val endpoint =
      s"${icebergEndpoint(ib)}${if (path.startsWith("/")) ""
        else "/"}$path"
    requests.get(endpoint, params = params)
  }

  def getIcebergConfig[E](
      ls: LocalStackV2Container,
      ib: GenericContainer,
      rowType: RowType,
      tableName: String,
      isSink: Boolean): String = {
    val sourceOrSink: String = if (isSink) "sink" else "source"
    s"""|${sourceOrSink}s {
        |   iceberg-$sourceOrSink {
        |    connector = iceberg
        |    row.type = "$rowType"
        |    catalog {
        |      name = iceberg
        |      uri = "${icebergEndpoint(ib)}"
        |      io-impl = "org.apache.iceberg.aws.s3.S3FileIO"
        |      s3.endpoint = "${s3Endpoint(ls)}"
        |      warehouse = "s3://$bucketName"
        |    }
        |    namespace = "testing.tables"
        |    table = "$tableName"
        |  }
        |}
        |""".stripMargin
  }

  def writeRows[E <: MySimpleADT: TypeInformation: ru.TypeTag](
      rows: Seq[E],
      rowType: RowType,
      tableName: String,
      ls: LocalStackV2Container,
      ib: GenericContainer): Unit = {
    val configStr =
      s"""
         |jobs { testJob {} }
         |${getIcebergConfig(ls, ib, rowType, tableName, isSink = true)}
         |""".stripMargin
    getIdentityTableStreamJobRunner[E, MySimpleADT](configStr, rows)
      .process()
  }

  def writeAvroRows[
      E <: MyAvroADT with EmbeddedAvroRecord[A]: TypeInformation,
      A <: GenericRecord: TypeInformation](
      rows: Seq[E],
      rowType: RowType,
      tableName: String,
      ls: LocalStackV2Container,
      ib: GenericContainer)(implicit
      fromKV: EmbeddedAvroRecordInfo[A] => E): Unit = {
    val configStr =
      s"""
         |jobs { testJob {} }
         |${getIcebergConfig(ls, ib, rowType, tableName, isSink = true)}
         |""".stripMargin
    getIdentityAvroStreamJobRunner[E, A, MyAvroADT](configStr, rows)
      .process()
  }
}
