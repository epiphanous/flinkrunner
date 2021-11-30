package io.epiphanous.flinkrunner.serde

import io.epiphanous.flinkrunner.model.{FlinkConfig, MyADT}

class MyADTConfluentAvroSerializationSchema(
    name: String,
    config: FlinkConfig[MyADT])
//    extends ConfluentAvroSerializationSchema {}
