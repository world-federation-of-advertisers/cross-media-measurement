package org.wfanet.measurement.common

import com.google.protobuf.MessageOrBuilder
import com.google.protobuf.ProtocolMessageEnum
import com.google.protobuf.Timestamp
import com.google.protobuf.util.JsonFormat
import java.time.Instant

/** Converts a protobuf [MessageOrBuilder] into its canonical JSON representation.*/
fun MessageOrBuilder.toJson(): String {
  return JsonFormat.printer().omittingInsignificantWhitespace().print(this)
}

fun Instant.toProtoTime(): Timestamp =
  Timestamp.newBuilder()
    .setSeconds(epochSecond)
    .setNanos(nano)
    .build()

fun Timestamp.toInstant(): Instant = Instant.ofEpochSecond(seconds, nanos.toLong())

val ProtocolMessageEnum.numberAsLong: Long
  get() = number.toLong()
