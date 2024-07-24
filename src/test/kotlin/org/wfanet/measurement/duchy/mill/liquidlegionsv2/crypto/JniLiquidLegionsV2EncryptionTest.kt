// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.mill.liquidlegionsv2.crypto

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.wfanet.anysketch.crypto.CombineElGamalPublicKeysRequest
import org.wfanet.measurement.internal.duchy.protocol.CompleteExecutionPhaseTwoRequest

class JniLiquidLegionsV2EncryptionTest {

  @Test
  fun `check JNI lib is loaded successfully`() {
    // Send an invalid request and check if we can get the error thrown inside JNI.
    val e1 =
      assertFailsWith(RuntimeException::class) {
        JniLiquidLegionsV2Encryption()
          .completeExecutionPhaseTwo(CompleteExecutionPhaseTwoRequest.getDefaultInstance())
      }
    assertThat(e1.message).contains("Parallelism must be greater than zero.")

    // Send an invalid request and check if we can get the error thrown inside JNI.
    val e2 =
      assertFailsWith(RuntimeException::class) {
        JniLiquidLegionsV2Encryption()
          .combineElGamalPublicKeys(CombineElGamalPublicKeysRequest.getDefaultInstance())
      }
    assertThat(e2.message).contains("Keys cannot be empty")
  }
}
