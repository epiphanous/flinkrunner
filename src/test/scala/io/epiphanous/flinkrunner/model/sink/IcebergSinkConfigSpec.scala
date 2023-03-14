package io.epiphanous.flinkrunner.model.sink

import com.dimafeng.testcontainers.lifecycle.and
import com.dimafeng.testcontainers.scalatest.TestContainersForAll
import com.dimafeng.testcontainers.{
  GenericContainer,
  LocalStackV2Container
}
import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.model.{BRecord, FlinkConfig, MyAvroADT}
import io.epiphanous.flinkrunner.util.AvroUtils.rowTypeOf
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.kinesis.shaded.software.amazon.awssdk.core.sync.ResponseTransformer
import org.apache.flink.table.types.logical.RowType
import org.testcontainers.containers.Network
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import org.testcontainers.utility.Base58
import requests.Response
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{
  CreateBucketRequest,
  GetObjectRequest,
  GetObjectResponse,
  ListObjectsV2Request
}

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

class IcebergSinkConfigSpec extends PropSpec with TestContainersForAll {

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

  def maybeCreateTableTest(
      ls: LocalStackV2Container,
      ib: GenericContainer,
      rowType: RowType): Unit = {
    val config     = new FlinkConfig(
      Array.empty[String],
      Some(s"""
           |sinks {
           |  iceberg-test {
           |    connector = "iceberg"
           |    catalog {
           |      name = iceberg
           |      uri = "${icebergEndpoint(ib)}"
           |      io-impl = "org.apache.iceberg.aws.s3.S3FileIO"
           |      s3.endpoint = "${s3Endpoint(ls)}"
           |      warehouse = "s3://$bucketName"
           |    }
           |    namespace = "testing.tables"
           |    table = "brecord"
           |  }
           |}
           |""".stripMargin)
    )
    val sinkConfig =
      new IcebergSinkConfig[MyAvroADT]("iceberg-test", config)
    val table      =
      sinkConfig.maybeCreateTable(sinkConfig.getFlinkTableSchema(rowType))
    table should be a 'Success
    logger.debug(table.get.toString)
  }

  // ****************** TESTS START HERE ************************

  property("has warehouse s3 bucket") {
    withContainers { case ls and _ =>
      val s3      = getS3Client(ls)
      val buckets = s3.listBuckets().buckets().asScala.toList
      logger.debug(buckets.toString)
      buckets should have size 1
      buckets.head.name() shouldEqual bucketName
    }
  }

  property("has iceberg catalog endpoint") {
    withContainers { case _ and ib =>
      val r: Response = icebergRest(ib, "/v1/config")
      logger.debug(r.text())
      r.statusCode shouldEqual 200
    }
  }

  property("create iceberg table") {
    withContainers { case ls and ib =>
      val rowType = rowTypeOf(classOf[BRecord]).fold(
        t =>
          throw new RuntimeException(
            "failed to compute RowType for BRecord",
            t
          ),
        rowType => rowType
      )
      logger.debug(rowType.toString)
      maybeCreateTableTest(ls, ib, rowType)
      val s3      = getS3Client(ls)
      val r       = s3.listObjectsV2(
        ListObjectsV2Request.builder().bucket(bucketName).build()
      )
      r.contents().asScala.foreach { o =>
        println(o.key())
        println(
          s3.getObjectAsBytes(
            GetObjectRequest
              .builder()
              .bucket(bucketName)
              .key(o.key())
              .build()
          ).asString(StandardCharsets.UTF_8)
        )
      }
    }
  }

  property("can write some rows") {
    withContainers { case ls and ib =>

    }
  }

}
