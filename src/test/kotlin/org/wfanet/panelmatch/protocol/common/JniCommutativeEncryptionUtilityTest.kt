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

package org.wfanet.panelmatch.protocol.common

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import wfanet.panelmatch.protocol.protobuf.ApplyCommutativeEncryptionRequest

class JniCommutativeEncryptionUtilityTest {

  @Test
  fun `check JNI lib is loaded successfully`() {
    // Send an invalid request and check if we can get the error thrown inside JNI.
    val e =
      assertFailsWith(RuntimeException::class) {
        JniCommutativeEncryption()
          .applyCommutativeEncryption(ApplyCommutativeEncryptionRequest.getDefaultInstance())
      }
    assertThat(e.message).contains("Failed to create the protocol cipher")
  }
}
