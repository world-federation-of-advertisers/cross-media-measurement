// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.client.common

import com.google.common.hash.Hashing
import com.google.protobuf.ByteString
import com.google.protobuf.Message
import com.google.protobuf.kotlin.toByteString

/** Helper for fingerprinting data. */
object Fingerprinters {

  /** Returns the FarmHash Fingerprint64 of [bytes]. */
  fun farmHashFingerprint64(bytes: ByteString): ByteString {
    return Hashing.farmHashFingerprint64().hashBytes(bytes.toByteArray()).asBytes().toByteString()
  }

  /** Returns the FarmHash Fingerprint64 of this [Message]. */
  fun Message.farmHashFingerprint64(): ByteString = farmHashFingerprint64(toByteString())
}
