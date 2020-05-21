package org.wfanet.measurement.common

import com.google.protobuf.Timestamp
import java.time.Instant

fun Instant.toProtoTime(): Timestamp =
  Timestamp.newBuilder()
    .setSeconds(epochSecond)
    .setNanos(nano)
    .build()

fun Timestamp.toInstant(): Instant = Instant.ofEpochSecond(seconds, nanos.toLong())
