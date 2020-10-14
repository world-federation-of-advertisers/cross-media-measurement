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

package org.wfanet.measurement.common.crypto

import com.google.protobuf.ByteString
import org.wfanet.measurement.common.size

data class ElGamalKeyPair(val publicKey: ElGamalPublicKey, val secretKey: ByteString) {
  init {
    require(secretKey.size == SECRET_KEY_SIZE)
  }

  companion object {
    /** The size of a [secretKey]. */
    const val SECRET_KEY_SIZE = 32
  }
}
