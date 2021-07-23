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

package org.wfanet.panelmatch.client.eventpreprocessing

import com.google.protobuf.ByteString
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.KV

/** Takes in a KV<ByteString,ByteString> and returns its size in bytes */
object EventSize : SerializableFunction<KV<ByteString, ByteString>, Int> {
  override fun apply(p: KV<ByteString, ByteString>): Int {
    return p.key.size() + p.value.size()
  }
}
