// Copyright 2020 The Measurement System Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.gcloud.spanner

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Type
import com.google.cloud.spanner.ValueBinder
import com.google.protobuf.ByteString
import com.google.protobuf.Message
import com.google.protobuf.Parser
import com.google.protobuf.ProtocolMessageEnum
import org.wfanet.measurement.common.numberAsLong
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.gcloud.common.parseFrom
import org.wfanet.measurement.gcloud.common.toGcloudByteArray

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

/** Returns the value of an Array of Structs column even if it is null. */
fun Struct.getNullableStructList(column: String): MutableList<Struct>? =
  nullOrValue(column, Type.Code.ARRAY, Struct::getStructList)

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

/**
 * Returns a bytes column as a protobuf ByteString.
 */
fun Struct.getBytesAsByteString(column: String): ByteString =
  ByteString.copyFrom(getBytes(column).asReadOnlyByteBuffer())

/** Parses a protobuf [Message] from a bytes column. */
fun <T : Message> Struct.getProtoMessage(column: String, parser: Parser<T>): T =
  parser.parseFrom(getBytes(column))

/** Parses an enum from an INT64 Spanner column. */
fun <T : Enum<T>> Struct.getProtoEnum(column: String, parser: (Int) -> T): T =
  parser(getLong(column).toInt())

/** Bind a protobuf [Message] as a Spanner ByteArray. */
fun <T> ValueBinder<T>.toProtoBytes(message: Message?): T = to(message?.toGcloudByteArray())

/** Bind a protobuf [Message] as a JSON string representation. */
fun <T> ValueBinder<T>.toProtoJson(message: Message?): T = to(message?.toJson())

/** Bind a protobuf enum to an INT64 Spanner column. */
fun <T> ValueBinder<T>.toProtoEnum(value: ProtocolMessageEnum): T = to(value.numberAsLong)
