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
import kotlin.test.assertFails
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.flatten
import org.wfanet.panelmatch.client.logger.TaskLog

private const val ATTEMPT_KEY = "some-arbitrary-attempt-key"

@RunWith(JUnit4::class)
class GenerateRandomBytesTaskTest {

  @Test
  fun numBytesMustBeGreaterThanZero() = withTestContext {
    assertFails { GenerateRandomBytesTask(numBytes = 0).execute(emptyMap()) }
    assertFails { GenerateRandomBytesTask(numBytes = -100).execute(emptyMap()) }
  }

  @Test
  fun generateBytes() = withTestContext {
    val outputs = GenerateRandomBytesTask(numBytes = 10).execute(emptyMap())
    val randomBytes = requireNotNull(outputs["random-bytes"]).flatten()
    assertThat(randomBytes.size()).isEqualTo(10)
  }
}

private fun withTestContext(block: suspend () -> Unit) {
  runBlocking(TaskLog(ATTEMPT_KEY) + Dispatchers.Default) { block() }
}
