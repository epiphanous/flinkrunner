package io.epiphanous.flinkrunner.avro

class AvroCodingException(message: String = "Failure during Avro coding", cause: Throwable = None.orNull)
    extends Exception(message, cause)
