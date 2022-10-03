package io.epiphanous.flinkrunner.model

import io.epiphanous.flinkrunner.serde.{
  DelimitedConfig,
  EmbeddedAvroDelimitedFileEncoder,
  EmbeddedAvroJsonFileEncoder,
  JsonConfig
}
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

  def readFile(path: String, isParquet: Boolean): List[BWrapper] =
    if (isParquet) readParquetFile(path) else readAvroFile(path)

  def readAvroFile(file: String): List[BWrapper] = {
    val fileSize                   = getFileInfoView(Paths.get(file)).readAttributes().size()
    val path                       = new Path(file)
    val inputFormat                =
      new EmbeddedAvroInputFormat[BWrapper, BRecord, MyAvroADT](path)
    inputFormat.open(new FileInputSplit(1, path, 0L, fileSize, null))
    val reuse: BWrapper            = BWrapper(new BRecord())
    val pop: ArrayBuffer[BWrapper] = ArrayBuffer.empty
    var done                       = false
    while (!done) {
      val b = inputFormat.nextRecord(reuse)
      done = b == null
      if (!done) pop += b
    }
    inputFormat.close()
    pop.toList
  }

  def readParquetFile(file: String): List[BWrapper] = {
    val fileSize                   = getFileInfoView(Paths.get(file)).readAttributes().size()
    val inputFormat                =
      new EmbeddedAvroParquetInputFormat[BWrapper, BRecord, MyAvroADT]()
    val input                      = new LocalDataInputStream(new File(file))
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
