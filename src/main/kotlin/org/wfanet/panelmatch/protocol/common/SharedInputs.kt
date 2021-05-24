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
import wfanet.panelmatch.protocol.protobuf.SharedInputs

/** Insert [data] into a [SharedInputs] proto and then serializes it. */
fun makeSerializedSharedInputs(data: List<ByteString>): ByteString {
  return SharedInputs.newBuilder().addAllData(data).build().toByteString()
}

/** Deserializes [data] as a [SharedInputs] proto and returns its `data` field. */
fun parseSerializedSharedInputs(data: ByteString): List<ByteString> {
  return SharedInputs.parseFrom(data).dataList
}
