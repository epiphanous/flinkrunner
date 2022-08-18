package io.epiphanous.flinkrunner.serde

import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.dataformat.csv.{CsvMapper, CsvSchema}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.epiphanous.flinkrunner.PropSpec
import io.epiphanous.flinkrunner.model.ARecord

class DelimitedConfigTest extends PropSpec {

  property("intoSchema empty property") {
    val delimitedConfig =
      DelimitedConfig.PSV.copy(columns = List("a", "b", "c"))
    val csvSchema       = delimitedConfig.intoSchema(CsvSchema.emptySchema())
    csvSchema.getColumnDesc shouldEqual """["a","b","c"]"""
  }

  property("intoSchema from type class property") {
    val mapper = CsvMapper
      .builder()
      .addModule(DefaultScalaModule)
      .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, false)
      .build()

    val delimitedConfig =
      DelimitedConfig.PSV.copy(columns = List("a", "b", "c", "d"))

    val start = mapper.schemaFor(classOf[ARecord])

    val csvSchema = delimitedConfig.intoSchema(start)

    csvSchema.getColumnDesc shouldEqual """["a","b","c","d"]"""
  }

}
