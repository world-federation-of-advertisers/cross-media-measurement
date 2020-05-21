package org.wfanet.measurement.db.gcp

import com.google.cloud.ByteArray
import com.google.protobuf.Message
import com.google.protobuf.MessageOrBuilder
import com.google.protobuf.Parser
import com.google.protobuf.util.JsonFormat

/** Converts a protobuf [MessageOrBuilder] into its canonical JSON representation.*/
fun MessageOrBuilder.toJson(): String {
  return JsonFormat.printer().omittingInsignificantWhitespace().print(this)
}

/** Converts a protobuf [Message] to a spanner [ByteArray]. */
fun Message.toSpannerByteArray(): ByteArray {
  return ByteArray.copyFrom(this.toByteArray())
}

/** Parse a spanner [ByteArray] into a protobuf [Message]. */
fun <T : Message> ByteArray.toProtobufMessage(parser: Parser<T>): T {
  return parser.parseFrom(this.toByteArray())
}
