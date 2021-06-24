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
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.launcher.testing.SINGLE_BLINDED_KEYS
import org.wfanet.panelmatch.protocol.common.makeSerializedSharedInputs
import org.wfanet.panelmatch.protocol.common.parseSerializedSharedInputs

@RunWith(JUnit4::class)
class IntersectValidateTaskTest {

  @Test
  fun `test valid intersect and validate exchange step`() = runBlocking {
    val output =
      IntersectValidateTask(maxSize = 100, minimumOverlap = 0.75f)
        .execute(
          mapOf(
            "previous-data" to makeSerializedSharedInputs(SINGLE_BLINDED_KEYS.dropLast(1)),
            "current-data" to makeSerializedSharedInputs(SINGLE_BLINDED_KEYS)
          )
        )
    assertThat(parseSerializedSharedInputs(requireNotNull(output["current-data"])))
      .isEqualTo(SINGLE_BLINDED_KEYS)
  }

  @Test
  fun `test data greater than max size fails to validate`() =
    runBlocking<Unit> {
      assertFailsWith(IllegalArgumentException::class) {
        IntersectValidateTask(maxSize = 1, minimumOverlap = 0.99f)
          .execute(
            mapOf(
              "previous-data" to makeSerializedSharedInputs(SINGLE_BLINDED_KEYS),
              "current-data" to makeSerializedSharedInputs(SINGLE_BLINDED_KEYS)
            )
          )
      }
    }

  @Test
  fun `test data with overlap below minimum overlap fails to validate`() =
    runBlocking<Unit> {
      assertFailsWith(IllegalArgumentException::class) {
        IntersectValidateTask(maxSize = 10, minimumOverlap = 0.85f)
          .execute(
            mapOf(
              "previous-data" to makeSerializedSharedInputs(SINGLE_BLINDED_KEYS.dropLast(1)),
              "current-data" to makeSerializedSharedInputs(SINGLE_BLINDED_KEYS)
            )
          )
      }
    }
}
