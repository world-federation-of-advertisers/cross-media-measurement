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

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.protocol.common.testing.AbstractCommutativeEncryptionTest
import wfanet.panelmatch.protocol.protobuf.ReApplyCommutativeEncryptionRequest

@RunWith(JUnit4::class)
class JniCommutativeEncryptionTest : AbstractCommutativeEncryptionTest() {
  override val commutativeEncryption: CommutativeEncryption = JniCommutativeEncryption()

  @Test
  fun `invalid proto throws JniException`() {
    val missingKeyException =
      assertFailsWith(JniCommutativeEncryption.JniException::class) {
        val request = ReApplyCommutativeEncryptionRequest.getDefaultInstance()
        commutativeEncryption.reEncrypt(request)
      }
    assertThat(missingKeyException.message).contains("Failed to create the protocol cipher")
  }
}
