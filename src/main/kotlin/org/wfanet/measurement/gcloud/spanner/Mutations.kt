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
import com.google.cloud.spanner.Mutation
import com.google.protobuf.Message
import com.google.protobuf.ProtocolMessageEnum

/** Sets the value that should be bound to the specified column. */
@JvmName("setBoolean")
fun Mutation.WriteBuilder.set(columnValuePair: Pair<String, Boolean>): Mutation.WriteBuilder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setBooleanBoxed")
fun Mutation.WriteBuilder.set(columnValuePair: Pair<String, Boolean?>): Mutation.WriteBuilder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setLong")
fun Mutation.WriteBuilder.set(columnValuePair: Pair<String, Long>): Mutation.WriteBuilder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setLongBoxed")
fun Mutation.WriteBuilder.set(columnValuePair: Pair<String, Long?>): Mutation.WriteBuilder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setDouble")
fun Mutation.WriteBuilder.set(columnValuePair: Pair<String, Double>): Mutation.WriteBuilder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setDoubleBoxed")
fun Mutation.WriteBuilder.set(columnValuePair: Pair<String, Double?>): Mutation.WriteBuilder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setString")
fun Mutation.WriteBuilder.set(columnValuePair: Pair<String, String?>): Mutation.WriteBuilder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setTimestamp")
fun Mutation.WriteBuilder.set(columnValuePair: Pair<String, Timestamp?>): Mutation.WriteBuilder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setDate")
fun Mutation.WriteBuilder.set(columnValuePair: Pair<String, Date?>): Mutation.WriteBuilder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setBytes")
fun Mutation.WriteBuilder.set(columnValuePair: Pair<String, ByteArray?>): Mutation.WriteBuilder {
  val (columnName, value) = columnValuePair
  return set(columnName).to(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setProtoEnum")
fun Mutation.WriteBuilder.set(
  columnValuePair: Pair<String, ProtocolMessageEnum>
): Mutation.WriteBuilder {
  val (columnName, value) = columnValuePair
  return set(columnName).toProtoEnum(value)
}

/** Sets the value that should be bound to the specified column. */
@JvmName("setProtoMessageBytes")
fun Mutation.WriteBuilder.set(columnValuePair: Pair<String, Message?>): Mutation.WriteBuilder {
  val (columnName, value) = columnValuePair
  return set(columnName).toProtoBytes(value)
}

/** Sets the JSON value that should be bound to the specified string column. */
fun Mutation.WriteBuilder.setJson(columnValuePair: Pair<String, Message?>): Mutation.WriteBuilder {
  val (columnName, value) = columnValuePair
  return set(columnName).toProtoJson(value)
}

/** Builds an [INSERT][Mutation.Op.INSERT] [Mutation]. */
inline fun insertMutation(table: String, bind: Mutation.WriteBuilder.() -> Unit): Mutation =
  Mutation.newInsertBuilder(table).apply(bind).build()

/** Builds an [UPDATE][Mutation.Op.UPDATE] [Mutation]. */
inline fun updateMutation(table: String, bind: Mutation.WriteBuilder.() -> Unit): Mutation =
  Mutation.newUpdateBuilder(table).apply(bind).build()

/** Builds an [INSERT_OR_UPDATE][Mutation.Op.INSERT_OR_UPDATE] [Mutation]. */
inline fun insertOrUpdateMutation(table: String, bind: Mutation.WriteBuilder.() -> Unit): Mutation =
  Mutation.newInsertOrUpdateBuilder(table).apply(bind).build()
