package io.epiphanous.flinkrunner.model

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.runtime.state.FunctionInitializationContext
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator

import java.time.Instant

class TestGeneratorFactory extends GeneratorFactory[MySimpleADT] {
  override def getDataGenerator[E <: MySimpleADT: TypeInformation](
      generatorConfig: GeneratorConfig): DataGenerator[E] = {
    new DataGenerator[E] {
      override def open(
          name: String,
          context: FunctionInitializationContext,
          runtimeContext: RuntimeContext): Unit = {}

      override def hasNext: Boolean = true

      override def next(): E =
        SimpleA("id", "a", 1, Instant.now()).asInstanceOf[E]
    }
  }
}
