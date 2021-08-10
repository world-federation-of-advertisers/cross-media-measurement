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

package org.wfanet.panelmatch.protocol.common

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.panelmatch.protocol.SharedInputs

/** Insert [data] into a [SharedInputs] proto and then serializes it. */
fun makeSerializedSharedInputs(data: List<ByteString>): ByteString {
  return SharedInputs.newBuilder().addAllData(data).build().toByteString()
}

/**
 * Converts the [ByteString] [List] into a [Flow] to allow for asynchronous operations on the
 * elements downstream.
 */
fun makeSerializedSharedInputFlow(data: List<ByteString>, bufferSize: Int): Flow<ByteString> {
  return makeSerializedSharedInputs(data).asBufferedFlow(bufferSize)
}

/** Deserializes [data] as a [SharedInputs] proto and returns its `data` field. */
fun parseSerializedSharedInputs(data: ByteString): List<ByteString> {
  return SharedInputs.parseFrom(data).dataList
}
