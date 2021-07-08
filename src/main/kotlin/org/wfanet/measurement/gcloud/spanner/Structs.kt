// Copyright 2021 The Cross-Media Measurement Authors
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

import com.google.cloud.ByteArray
import com.google.cloud.Date
import com.google.cloud.Timestamp
import com.google.cloud.spanner.Struct
import com.google.protobuf.Message
import com.google.protobuf.ProtocolMessageEnum

/** Sets the value that should be bound to the specified column. */
@JvmName("setBoolean")
fun Struct.Builder.set(columnValuePair: Pair<String, Boolean>): Struct.Builder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setBooleanBoxed")
fun Struct.Builder.set(columnValuePair: Pair<String, Boolean?>): Struct.Builder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setLong")
fun Struct.Builder.set(columnValuePair: Pair<String, Long>): Struct.Builder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setLongBoxed")
fun Struct.Builder.set(columnValuePair: Pair<String, Long?>): Struct.Builder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setDouble")
fun Struct.Builder.set(columnValuePair: Pair<String, Double>): Struct.Builder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setDoubleBoxed")
fun Struct.Builder.set(columnValuePair: Pair<String, Double?>): Struct.Builder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setString")
fun Struct.Builder.set(columnValuePair: Pair<String, String?>): Struct.Builder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setTimestamp")
fun Struct.Builder.set(columnValuePair: Pair<String, Timestamp?>): Struct.Builder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setDate")
fun Struct.Builder.set(columnValuePair: Pair<String, Date?>): Struct.Builder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setBytes")
fun Struct.Builder.set(columnValuePair: Pair<String, ByteArray?>): Struct.Builder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setProtoEnum")
fun Struct.Builder.set(columnValuePair: Pair<String, ProtocolMessageEnum>): Struct.Builder {
  val (columnName, value) = columnValuePair
  return set(columnName).toProtoEnum(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setProtoMessageBytes")
fun Struct.Builder.set(columnValuePair: Pair<String, Message?>): Struct.Builder {
  val (columnName, value) = columnValuePair
  return set(columnName).toProtoBytes(value)
}

/** Sets the JSON value that should be bound to the specified string column. */
fun Struct.Builder.setJson(columnValuePair: Pair<String, Message?>): Struct.Builder {
  val (columnName, value) = columnValuePair
  return set(columnName).toProtoJson(value)
}

/** Builds an [Struct]. */
inline fun makeStruct(bind: Struct.Builder.() -> Unit): Struct =
  Struct.newBuilder().apply(bind).build()
