package org.wfanet.measurement.db.gcp

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Type
import com.google.cloud.spanner.ValueBinder
import com.google.protobuf.Message
import com.google.protobuf.Parser
import com.google.protobuf.ProtocolMessageEnum
import org.wfanet.measurement.common.numberAsLong
import org.wfanet.measurement.common.toJson

private fun <T> Struct.nullOrValue(
  column: String,
  typeCode: Type.Code,
  getter: Struct.(String) -> T
): T? {
  val columnType = getColumnType(column).code
  check(columnType == typeCode) { "Cannot read $typeCode from $column, it has type $columnType" }
  return if (isNull(column)) null else getter(column)
}

/** Returns the value of a String column even if it is null. */
fun Struct.getNullableString(column: String): String? =
  nullOrValue(column, Type.Code.STRING, Struct::getString)

/** Returns the value of a Timestamp column even if it is null. */
fun Struct.getNullableTimestamp(column: String): Timestamp? =
  nullOrValue(column, Type.Code.TIMESTAMP, Struct::getTimestamp)

/** Returns the value of a INT64 column even if it is null. */
fun Struct.getNullableLong(column: String): Long? =
  nullOrValue(column, Type.Code.INT64, Struct::getLong)

/**
 * Returns a bytes column as a Kotlin native ByteArray. This is useful for deserializing protos.
 */
fun Struct.getBytesAsByteArray(column: String): ByteArray = getBytes(column).toByteArray()

/** Parses a protobuf [Message] from a bytes column. */
fun <T : Message> Struct.getProtoBufMessage(column: String, parser: Parser<T>): T =
  getBytes(column).toProtobufMessage(parser)

/** Parses an enum from an INT64 Spanner column. */
fun <T : Enum<T>> Struct.getProtoEnum(column: String, parser: (Int) -> T): T =
  parser(getLong(column).toInt())

/** Bind a protobuf [Message] as a Spanner ByteArray. */
fun <T> ValueBinder<T>.toProtoBytes(message: Message?): T = to(message?.toSpannerByteArray())

/** Bind a protobuf [Message] as a JSON string representation. */
fun <T> ValueBinder<T>.toProtoJson(message: Message?): T = to(message?.toJson())

/** Bind a protobuf enum to an INT64 Spanner column. */
fun <T> ValueBinder<T>.toProtoEnum(value: ProtocolMessageEnum): T = to(value.numberAsLong)
