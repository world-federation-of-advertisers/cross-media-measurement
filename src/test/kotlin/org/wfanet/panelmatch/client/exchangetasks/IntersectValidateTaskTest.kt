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
import com.google.protobuf.ByteString
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.launcher.testing.SINGLE_BLINDED_KEYS
import org.wfanet.panelmatch.client.storage.InMemoryStorageClient
import org.wfanet.panelmatch.protocol.common.makeSerializedSharedInputFlow
import org.wfanet.panelmatch.protocol.common.parseSerializedSharedInputs

@RunWith(JUnit4::class)
class IntersectValidateTaskTest {
  private val mockStorage = InMemoryStorageClient(keyPrefix = "mock")

  @Test
  fun `test valid intersect and validate exchange step`() = runBlocking {
    val output =
      IntersectValidateTask(maxSize = 100, minimumOverlap = 0.75f)
        .execute(
          mapOf(
            "previous-data" to
              mockStorage.createBlob(
                "encrypted-data",
                makeSerializedSharedInputFlow(
                  SINGLE_BLINDED_KEYS.dropLast(1),
                  mockStorage.defaultBufferSizeBytes
                )
              ),
            "current-data" to
              mockStorage.createBlob(
                "current-data",
                makeSerializedSharedInputFlow(
                  SINGLE_BLINDED_KEYS,
                  mockStorage.defaultBufferSizeBytes
                )
              )
          )
        )
    assertThat(
        parseSerializedSharedInputs(
          requireNotNull(output["current-data"])
            .fold(ByteString.EMPTY, { agg, chunk -> agg.concat(chunk) })
        )
      )
      .isEqualTo(SINGLE_BLINDED_KEYS)
  }

  @Test
  fun `test data greater than max size fails to validate`() =
    runBlocking<Unit> {
      assertFailsWith(IllegalArgumentException::class) {
        IntersectValidateTask(maxSize = 1, minimumOverlap = 0.99f)
          .execute(
            mapOf(
              "previous-data" to
                mockStorage.createBlob(
                  "current-data",
                  makeSerializedSharedInputFlow(
                    SINGLE_BLINDED_KEYS,
                    mockStorage.defaultBufferSizeBytes
                  )
                ),
              "current-data" to
                mockStorage.createBlob(
                  "current-data",
                  makeSerializedSharedInputFlow(
                    SINGLE_BLINDED_KEYS,
                    mockStorage.defaultBufferSizeBytes
                  )
                )
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
              "previous-data" to
                mockStorage.createBlob(
                  "current-data",
                  makeSerializedSharedInputFlow(
                    SINGLE_BLINDED_KEYS.dropLast(1),
                    mockStorage.defaultBufferSizeBytes
                  )
                ),
              "current-data" to
                mockStorage.createBlob(
                  "current-data",
                  makeSerializedSharedInputFlow(
                    SINGLE_BLINDED_KEYS,
                    mockStorage.defaultBufferSizeBytes
                  )
                )
            )
          )
      }
    }
}
