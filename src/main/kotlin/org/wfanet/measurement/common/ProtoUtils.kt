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

package org.wfanet.measurement.common

import com.google.protobuf.ByteString
import com.google.protobuf.Descriptors
import com.google.protobuf.Message
import com.google.protobuf.MessageOrBuilder
import com.google.protobuf.ProtocolMessageEnum
import com.google.protobuf.Timestamp
import com.google.protobuf.util.JsonFormat
import java.time.Clock
import java.time.Instant

/** Converts a protobuf [MessageOrBuilder] into its canonical JSON representation.*/
fun MessageOrBuilder.toJson(): String {
  return JsonFormat.printer().omittingInsignificantWhitespace().print(this)
}

/** Truncate all byte fields inside a protobuf [Message].*/
fun Message.truncateByteFields(truncatedSize: Int): Message {
  val builder = this.toBuilder()
  for (descriptor in this.descriptorForType.fields) {
    when (descriptor.type) {
      Descriptors.FieldDescriptor.Type.BYTES -> {
        if (!descriptor.isRepeated) {
          val bytes = this.getField(descriptor) as ByteString
          builder.setField(descriptor, bytes.substring(0, minOf(bytes.size(), truncatedSize)))
        } else {
          val bytesList = this.getField(descriptor) as List<*>
          builder.clearField(descriptor)
          for (bytes in bytesList) {
            builder.addRepeatedField(
              descriptor, (bytes as ByteString).substring(0, minOf(bytes.size(), truncatedSize))
            )
          }
        }
      }
      Descriptors.FieldDescriptor.Type.MESSAGE -> {
        if (!descriptor.isRepeated) {
          val message = this.getField(descriptor) as Message
          builder.setField(descriptor, message.truncateByteFields(truncatedSize))
        } else {
          val messages = this.getField(descriptor) as List<*>
          builder.clearField(descriptor)
          for (message in messages) {
            builder.addRepeatedField(
              descriptor,
              (message as Message).truncateByteFields(truncatedSize)
            )
          }
        }
      }
      else -> {} // do nothing
    }
  }
  return builder.build()
}

fun Instant.toProtoTime(): Timestamp =
  Timestamp.newBuilder()
    .setSeconds(epochSecond)
    .setNanos(nano)
    .build()

fun Timestamp.toInstant(): Instant = Instant.ofEpochSecond(seconds, nanos.toLong())

fun Clock.protoTimestamp(): Timestamp = instant().toProtoTime()

val ProtocolMessageEnum.numberAsLong: Long
  get() = number.toLong()
