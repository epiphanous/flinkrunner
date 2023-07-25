package io.epiphanous.flinkrunner.util

import com.typesafe.scalalogging.LazyLogging
import io.epiphanous.flinkrunner.model.{DataTypeConfig, FlinkEvent}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter
import org.apache.flink.table.annotation.DataTypeHint
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.table.types.logical.utils.LogicalTypeParser

import scala.collection.JavaConverters._
import scala.reflect.runtime.{universe => ru}
import scala.util.{Failure, Success, Try}

object RowUtils extends LazyLogging {

  /** Infer a RowType for a FlinkEvent of type E. The algorithm here
    * leverages scala runtime reflection, so you need to make sure a
    * TypeTag is available implicitly. Usually, you simply need to add a
    * TypeTag bound on E where you declare your job.
    *
    * If the event type is an EmbeddedAvroRecord, it will extract the avro
    * schema from the type definition and create a RowType from the avro
    * schema. Otherwise, it will reflect on the class' primary constructor
    * and define a schema for each field in its first param list.
    * @tparam E
    *   the flink event type
    * @return
    *   RowType
    */
  def rowTypeOf[E <: FlinkEvent: ru.TypeTag]: RowType = {
    val t = ru.typeOf[E]
    t.decls
      .find(d =>
        d.isMethod && d.asMethod.name.toString.contentEquals("$record")
      )
      .flatMap { k =>
        schemaOf(k.asMethod.returnType.typeSymbol.asClass) match {
          case Success(schema) => Some(schema)
          case Failure(ex)     =>
            logger.error("failed to get schema", ex)
            None
        }
      }
      .map(schema =>
        AvroSchemaConverter
          .convertToDataType(schema.toString)
      )
      .getOrElse(
        getRowTypeOf(t).notNull()
      )
      .getLogicalType
      .asInstanceOf[RowType]
  }

  private def schemaOf(klass: ru.ClassSymbol): Try[Schema] = Try {
    val cm                = scala.reflect.runtime.currentMirror.reflectClass(klass)
    // find zero-arg constructor
    val constructor       =
      cm.symbol.info.decls
        .find(p =>
          p.isMethod && {
            val m = p.asMethod
            m.isConstructor && m.paramLists.flatten.isEmpty
          }
        )
        .get
        .asMethod
    val constructorMethod = cm.reflectConstructor(constructor)
    constructorMethod().asInstanceOf[GenericRecord].getSchema
  }

  private def primaryConstructor(scalaType: ru.Type): Seq[ru.Symbol] = {
    scalaType.decls
      .find(d => d.isMethod && d.asMethod.isPrimaryConstructor)
      .get
      .typeSignature
      .paramLists
      .head
  }

  private def getRowTypeOf(scalaType: ru.Type): DataType = {
    val fields = primaryConstructor(scalaType)
      .map(f =>
        DataTypes.FIELD(
          f.name.toString,
          fieldType(
            f.typeSignature,
            f.annotations
              .find(a => a.tree.tpe <:< ru.typeOf[DataTypeHint])
              .map(parseDataTypeConfig)
              .getOrElse(DataTypeConfig())
          )
        )
      )
      .asJava
    DataTypes.ROW(fields)
  }

  private def fieldType(
      scalaType: ru.Type,
      dataTypeConfig: DataTypeConfig): DataType = {
    dataTypeConfig.value
      .map(t =>
        DataTypes.of(LogicalTypeParser.parse(t, getClass.getClassLoader))
      )
      .getOrElse {
        scalaType.typeSymbol.fullName match {
          case "java.lang.String"                                     => DataTypes.STRING().notNull()
          case "scala.Boolean" | "java.lang.Boolean"                  =>
            DataTypes.BOOLEAN().notNull()
          case "scala.Byte" | "java.lang.Byte"                        =>
            DataTypes.TINYINT().notNull()
          case "scala.Short" | "java.lang.Short"                      =>
            DataTypes.SMALLINT().notNull()
          case "scala.Int" | "java.lang.Integer"                      =>
            DataTypes.INT().notNull()
          case "scala.Long" | "java.lang.Long"                        =>
            DataTypes.BIGINT().notNull()
          case "scala.BigInt" | "java.math.BigInteger"                =>
            DataTypes.DECIMAL(
              dataTypeConfig.defaultDecimalPrecision.getOrElse(10),
              dataTypeConfig.defaultDecimalScale.getOrElse(0)
            )
          case "scala.Float" | "java.lang.Float"                      =>
            DataTypes.FLOAT().notNull()
          case "scala.Double"                                         => DataTypes.DOUBLE().notNull()
          case "scala.BigDecimal" | "java.math.BigDecimal"            =>
            DataTypes.DECIMAL(
              dataTypeConfig.defaultDecimalPrecision.getOrElse(10),
              dataTypeConfig.defaultDecimalScale.getOrElse(0)
            )
          case "java.time.Instant" | "java.sql.Timestamp"             =>
            dataTypeConfig.defaultSecondPrecision
              .map(p => DataTypes.TIMESTAMP_LTZ(p))
              .getOrElse(DataTypes.TIMESTAMP_LTZ())
              .notNull()
          case "java.time.LocalDateTime"                              =>
            dataTypeConfig.defaultSecondPrecision
              .map(p => DataTypes.TIMESTAMP(p))
              .getOrElse(DataTypes.TIMESTAMP())
              .notNull()
          case "java.time.OffsetDateTime" | "java.time.ZonedDateTime" =>
            dataTypeConfig.defaultSecondPrecision
              .map(p => DataTypes.TIMESTAMP_WITH_TIME_ZONE(p))
              .getOrElse(DataTypes.TIMESTAMP_WITH_TIME_ZONE())
              .notNull()
          case "java.time.LocalDate" | "java.sql.Date"                =>
            DataTypes.DATE().notNull()
          case "java.time.LocalTime" | "java.sql.Time"                =>
            dataTypeConfig.defaultSecondPrecision
              .map(p => DataTypes.TIME(p))
              .getOrElse(DataTypes.TIME())
              .notNull()
          case "scala.Option"                                         =>
            fieldType(scalaType.typeArgs.head, dataTypeConfig).nullable()
          case "java.nio.ByteBuffer"                                  => DataTypes.BYTES().notNull()
          case "scala.Array"
              if scalaType.typeArgs.head.typeSymbol.fullName.contentEquals(
                "scala.Byte"
              ) =>
            DataTypes.BYTES().notNull()
          case _ if isArray(scalaType)                                =>
            DataTypes.ARRAY(
              fieldType(scalaType.typeArgs.head, dataTypeConfig)
            )
          case _ if isMap(scalaType)                                  =>
            DataTypes.MAP(
              fieldType(scalaType.typeArgs.head, dataTypeConfig),
              fieldType(scalaType.typeArgs.last, dataTypeConfig)
            )
          case _ if isRow(scalaType)                                  =>
            getRowTypeOf(scalaType)
        }
      }
  }

  private def parseDataTypeConfig(a: ru.Annotation): DataTypeConfig =
    a.tree.children.tail.foldLeft(DataTypeConfig()) {
      case (
            dtc,
            _ @ru.AssignOrNamedArg(ru.Ident(name), ru.Literal(lit))
          ) =>
        val prop = name.toString
        (prop, lit) match {
          case ("value", ru.Constant(v: String))                 =>
            dtc.copy(value = Some(v))
          case ("defaultDecimalPrecision", ru.Constant(dp: Int)) =>
            dtc.copy(defaultDecimalPrecision = Some(dp))
          case ("defaultDecimalScale", ru.Constant(ds: Int))     =>
            dtc.copy(defaultDecimalPrecision = Some(ds))
          case ("defaultSecondPrecision", ru.Constant(sp: Int))  =>
            dtc.copy(defaultSecondPrecision = Some(sp))
          case ("defaultYearPrecision", ru.Constant(yp: Int))    =>
            dtc.copy(defaultSecondPrecision = Some(yp))
          case ("bridgedTo", ru.Constant(bt: Class[_]))          =>
            dtc.copy(bridgedTo = Some(bt))
          case _                                                 =>
            throw new RuntimeException(s"unsupported literal type $lit")
        }
    }

  private def isArray(scalaType: ru.Type): Boolean = ???

  private def isMap(scalaType: ru.Type): Boolean = ???

  private def isRow(scalaType: ru.Type): Boolean = ???

  private def isa(scalaType: ru.Type, parentClass: String): Boolean =
    scalaType.baseClasses.map(_.fullName).contains(parentClass)

}
