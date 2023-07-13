package io.epiphanous.flinkrunner.serde

import com.amazonaws.services.schemaregistry.flink.avro.GlueSchemaRegistryAvroSchemaCoderProvider
import io.epiphanous.flinkrunner.util.AvroUtils.parseSchemaString
import org.apache.avro.Schema
import org.apache.flink.formats.avro.RegistryAvroDeserializationSchema
import org.apache.flink.formats.avro.SchemaCoder.SchemaCoderProvider

import java.util

class MyGlueRegistryAvroDeserializationSchema[A](
    clazz: Class[A],
    schemaOpt: Option[Schema],
    schemaCoderProvider: SchemaCoderProvider)
    extends RegistryAvroDeserializationSchema[A](
      clazz,
      schemaOpt.orNull,
      schemaCoderProvider
    )

/** This exists because flink sends in a null subject value when
  * interacting with confluent's schema registry and that doesn't play nice
  * with an ecosystem where people expect a relationship between the
  * subject name and the schema name
  */
object MyGlueRegistryAvroDeserializationSchema {

  def forKey(configs: util.Map[String, String]) =
    new MyGlueRegistryAvroDeserializationSchema[String](
      classOf[String],
      Some(Schema.create(Schema.Type.STRING)),
      new GlueSchemaRegistryAvroSchemaCoderProvider(
        configs.asInstanceOf[util.Map[String, AnyRef]]
      )
    )

  def forValue[A](
      clazz: Class[A],
      configs: util.Map[String, String],
      readerSchema: Option[String] = None
  ) = new MyGlueRegistryAvroDeserializationSchema[A](
    clazz,
    readerSchema.map(parseSchemaString),
    new GlueSchemaRegistryAvroSchemaCoderProvider(
      configs.asInstanceOf[util.Map[String, AnyRef]]
    )
  )
}
