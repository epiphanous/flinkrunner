package io.epiphanous.flinkrunner.model.sink

import com.dimafeng.testcontainers.lifecycle.and
import com.dimafeng.testcontainers.scalatest.TestContainersForAll
import com.dimafeng.testcontainers.{
  GenericContainer,
  LocalStackV2Container
}
import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.model.{BRecord, MyAvroADT}
import io.epiphanous.flinkrunner.util.AvroUtils.rowTypeOf
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.table.types.logical.RowType
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import requests.Response
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{
  CreateBucketRequest,
  ListObjectsRequest,
  ListObjectsV2Request
}

import scala.collection.JavaConverters._

class IcebergSinkConfigSpec extends PropSpec with TestContainersForAll {

  val bucketName      = "warehouse"
  val icebergRESTPort = 8181

  val icebergImage: GenericContainer.DockerImage =
    GenericContainer.stringToDockerImage("tabulario/iceberg-rest:latest")

  override type Containers = LocalStackV2Container and GenericContainer

  override def startContainers(): Containers = {
    val localstack     = LocalStackV2Container
      .Def(tag = "1.4.0", services = Seq(Service.S3))
      .start()
    val creds          = localstack.staticCredentialsProvider.resolveCredentials()
    val env            = Map(
      "CATALOG_S3_ACCESS__KEY__ID"     -> creds.accessKeyId(),
      "CATALOG_S3_SECRET__ACCESS__KEY" -> creds.secretAccessKey(),
      "AWS_REGION"                     -> localstack.region.toString,
      "CATALOG_WAREHOUSE"              -> s"s3://$bucketName/",
      "CATALOG_IO__IMPL"               -> "org.apache.iceberg.aws.s3.S3FileIO",
      "CATALOG_S3_ENDPOINT"            -> s3Endpoint(localstack),
      "CATALOG_S3_PATH__STYLE__ACCESS" -> "true"
    )
    val icebergCatalog = GenericContainer
      .Def(
        dockerImage = icebergImage,
        exposedPorts = Seq(icebergRESTPort),
        env = env
      )
      .start()
    localstack and icebergCatalog
  }

  import org.apache.iceberg.jdbc.JdbcCatalog
  import org.apache.iceberg.aws.s3.S3FileIO
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

  def s3Endpoint(ls: LocalStackV2Container): String =
    ls.endpointOverride(Service.S3).toString

  def icebergRestEndpoint(ib: GenericContainer): String = {
    val mappedPort = ib.container.getMappedPort(icebergRESTPort)
    s"http://localhost:$mappedPort"
  }

  def icebergRest(
      ib: GenericContainer,
      path: String,
      params: Iterable[(String, String)] = Nil): Response = {
    val endpoint =
      s"${icebergRestEndpoint(ib)}${if (path.startsWith("/")) ""
        else "/"}$path"
    requests.get(endpoint, params = params)
  }

  def maybeCreateTableTest(
      ls: LocalStackV2Container,
      ib: GenericContainer,
      rowType: RowType): Unit = {
    logger.debug(s"iceberg rest endpoint: ${icebergRestEndpoint(ib)}")
    logger.debug(s"s3 endpoint: ${s3Endpoint(ls)}")
    val runner     = getRunner[MyAvroADT](
      Array.empty[String],
      Some(s"""
           |sinks {
           |  iceberg-test {
           |    connector = "iceberg"
           |    catalog {
           |      name = iceberg
           |      uri = "${icebergRestEndpoint(ib)}"
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
      new IcebergSinkConfig[MyAvroADT]("iceberg-test", runner.config)
    val table      =
      sinkConfig.maybeCreateTable(sinkConfig.getFlinkTableSchema(rowType))
    table should be a 'Success
    logger.debug(table.get.toString)
  }

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
      if (r.hasContents) println("Got Some!")
      logger.debug(r.toString)
      r.contents().asScala.foreach(o => println(o.toString))
    }
  }

}
