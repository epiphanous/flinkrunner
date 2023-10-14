package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.model.source.{
  FileSourceConfig,
  SourceConfig
}
import io.epiphanous.flinkrunner.serde.{
  DelimitedConfig,
  EmbeddedAvroDelimitedFileEncoder,
  EmbeddedAvroJsonFileEncoder,
  JsonConfig
}
import io.epiphanous.flinkrunner.util.AvroUtils.toEmbeddedAvroInstance
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.serialization.BulkWriter
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.local.LocalDataInputStream
import org.apache.flink.core.fs.{
  FSDataOutputStream,
  FileInputSplit,
  FileSystem,
  Path
}
import org.apache.flink.formats.avro.AvroInputFormat
import org.apache.flink.testutils.TestFileSystem

import java.io.File
import java.nio.file
import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.{Files, Paths}
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

trait AvroFileTestUtils {

  val overwrite = FileSystem.WriteMode.OVERWRITE

  def getStream(path: String): FSDataOutputStream =
    new TestFileSystem().create(new Path(path), overwrite)

  def getWriter(path: String, isParquet: Boolean): BulkWriter[BWrapper] = {
    new EmbeddedAvroWriterFactory[BWrapper, BRecord, MyAvroADT](
      isParquet
    ).create(getStream(path))
  }

  def readFile(path: String, isParquet: Boolean): List[BWrapper] = {
    val config                                 = new FlinkConfig(
      Array("test-read-file"),
      Some(s"""
        |sources {
        |  test-read-file-source {
        |    path = "$path"
        |    format = ${if (isParquet) "parquet" else "avro"}
        |  }
        |}
        |""".stripMargin)
    )
    val srcConfig: FileSourceConfig[MyAvroADT] =
      SourceConfig[MyAvroADT]("test-read-file-source", config)
        .asInstanceOf[FileSourceConfig[MyAvroADT]]
    if (isParquet) readParquetFile(srcConfig)
    else readAvroFile(srcConfig)
  }

  def readAvroFile(
      srcConfig: FileSourceConfig[MyAvroADT]): List[BWrapper] = {
    val fileSize                   =
      getFileInfoView(Paths.get(srcConfig.path)).readAttributes().size()
    val inputFormat                =
      new AvroInputFormat[GenericRecord](
        srcConfig.origin,
        classOf[GenericRecord]
      )
    inputFormat.open(
      new FileInputSplit(1, srcConfig.origin, 0L, fileSize, null)
    )
    val reuse: BRecord             = new BRecord()
    val pop: ArrayBuffer[BWrapper] = ArrayBuffer.empty
    var done                       = false
    while (!done) {
      val b = inputFormat.nextRecord(reuse)
      done = b == null
      if (!done)
        toEmbeddedAvroInstance[BWrapper, BRecord, MyAvroADT](
          b,
          classOf[BRecord],
          srcConfig.config
        ).foreach(bw => pop += bw)
    }
    inputFormat.close()
    pop.toList
  }

  def readParquetFile(
      srcConfig: FileSourceConfig[MyAvroADT]): List[BWrapper] = {
    val fileSize                   =
      getFileInfoView(Paths.get(srcConfig.path)).readAttributes().size()
    val inputFormat                =
      new EmbeddedAvroParquetInputFormat[BWrapper, BRecord, MyAvroADT](
        srcConfig
      )
    val input                      = new LocalDataInputStream(new File(srcConfig.path))
    val reader                     = inputFormat.createReader(
      new Configuration(),
      input,
      fileSize,
      fileSize
    )
    val pop: ArrayBuffer[BWrapper] = ArrayBuffer.empty
    var done                       = false
    while (!done) {
      val b = reader.read()
      done = b == null
      if (!done) pop += b
    }
    reader.close()
    pop.toList
  }

  def writeBulkFile(
      path: String,
      isParquet: Boolean,
      pop: List[BWrapper]): Unit = {
    val writer = getWriter(path, isParquet)
    pop.foreach(writer.addElement)
    writer.finish()
  }

  def writeFile(
      path: String,
      format: StreamFormatName,
      pop: List[BWrapper],
      delimitedConfig: DelimitedConfig = DelimitedConfig.CSV,
      jsonConfig: JsonConfig = JsonConfig()): Unit = {
    val stream           = getStream(path)
    val (mapper, finish) = format match {
      case StreamFormatName.Json =>
        val encoder =
          new EmbeddedAvroJsonFileEncoder[BWrapper, BRecord, MyAvroADT](
            jsonConfig
          )
        ((bw: BWrapper) => encoder.encode(bw, stream), () => ())

      case fmt if fmt.isDelimited =>
        val encoder = new EmbeddedAvroDelimitedFileEncoder[
          BWrapper,
          BRecord,
          MyAvroADT
        ](delimitedConfig)
        ((bw: BWrapper) => encoder.encode(bw, stream), () => ())

      case fmt if fmt.isBulk =>
        val writer = getWriter(path, fmt == StreamFormatName.Parquet)
        ((bw: BWrapper) => writer.addElement(bw), () => writer.finish())
    }
    pop.foreach(mapper)
    finish()
    stream.flush()
    stream.close()
  }

  def getFileInfoView(path: java.nio.file.Path): BasicFileAttributeView =
    Files
      .getFileAttributeView(
        path,
        classOf[BasicFileAttributeView]
      )

  def getTempBulkFile(isParquet: Boolean): Try[java.nio.file.Path] =
    getTempFile(
      if (isParquet) StreamFormatName.Parquet else StreamFormatName.Avro
    )

  def getTempFile(format: StreamFormatName): Try[file.Path] = {
    val x = format.entryName.toLowerCase
    Try(Files.createTempFile(s"$x-test-", s".$x"))
  }
}
