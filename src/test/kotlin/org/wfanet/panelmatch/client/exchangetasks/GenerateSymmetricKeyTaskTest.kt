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
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.flatten
import org.wfanet.panelmatch.common.crypto.testing.FakeDeterministicCommutativeCipher

private const val ATTEMPT_KEY = "some-arbitrary-attempt-key"

@RunWith(JUnit4::class)
class GenerateSymmetricKeyTaskTest {
  private val deterministicCommutativeCryptor = FakeDeterministicCommutativeCipher()

  @Test
  fun `generate key 2x yields different keys`() = withTestContext {
    val result1 =
      GenerateSymmetricKeyTask(generateKey = deterministicCommutativeCryptor::generateKey)
        .execute(emptyMap())
        .mapValues { it.value.flatten() }
    val result2 =
      GenerateSymmetricKeyTask(generateKey = deterministicCommutativeCryptor::generateKey)
        .execute(emptyMap())
        .mapValues { it.value.flatten() }

    assertThat(result1.getValue("symmetric-key")).isNotEqualTo(result2.getValue("symmetric-key"))
  }
}

private fun withTestContext(block: suspend () -> Unit) {
  runBlocking { withContext(CoroutineName(ATTEMPT_KEY) + Dispatchers.Default) { block() } }
}
