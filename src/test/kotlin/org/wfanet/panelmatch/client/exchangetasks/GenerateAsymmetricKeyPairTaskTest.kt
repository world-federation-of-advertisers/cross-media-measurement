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

package org.wfanet.panelmatch.client.exchangetasks

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.flatten
import org.wfanet.panelmatch.client.logger.TaskLog
import org.wfanet.panelmatch.client.privatemembership.testing.PlaintextPrivateMembershipCryptor

private const val ATTEMPT_KEY = "some-arbitrary-attempt-key"

@RunWith(JUnit4::class)
class GenerateAsymmetricKeyPairTaskTest {
  private val keyGenerator = PlaintextPrivateMembershipCryptor()::generateKeys

  @Test
  fun `public key is not equal to private key`() = withTestContext {
    val result =
      GenerateAsymmetricKeyPairTask(generateKeys = keyGenerator).execute(emptyMap()).mapValues {
        it.value.flatten()
      }

    assertThat(result.getValue("public-key")).isEqualTo("some public key".toByteStringUtf8())
    assertThat(result.getValue("private-key")).isEqualTo("some private key".toByteStringUtf8())
  }
}

private fun withTestContext(block: suspend () -> Unit) {
  runBlocking(TaskLog(ATTEMPT_KEY) + Dispatchers.Default) { block() }
}
