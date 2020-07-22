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
