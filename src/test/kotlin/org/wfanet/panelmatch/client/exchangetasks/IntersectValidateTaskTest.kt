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
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.panelmatch.client.launcher.testing.SINGLE_BLINDED_KEYS
import org.wfanet.panelmatch.common.storage.createBlob
import org.wfanet.panelmatch.common.toByteString

@RunWith(JUnit4::class)
class IntersectValidateTaskTest {
  private val mockStorage = InMemoryStorageClient()
  private val numSingleBlindedKeys = SINGLE_BLINDED_KEYS.size
  private val singleBlindedKeysAndIds =
    SINGLE_BLINDED_KEYS.zip(1..SINGLE_BLINDED_KEYS.size) { singleBlindedKey, keyId ->
      joinKeyAndId {
        this.joinKey = joinKey { key = singleBlindedKey }
        this.joinKeyIdentifier = joinKeyIdentifier { id = "joinKeyId of $keyId".toByteString() }
      }
    }
  private val singleBlindedKeysAndWrongIds =
    SINGLE_BLINDED_KEYS.zip(SINGLE_BLINDED_KEYS.size..1) { singleBlindedKey, keyId ->
      joinKeyAndId {
        this.joinKey = joinKey { key = singleBlindedKey }
        this.joinKeyIdentifier = joinKeyIdentifier { id = "joinKeyId of $keyId".toByteString() }
      }
    }
  private val blobOfSingleBlindedKeys = runBlocking {
    mockStorage.createBlob(
      "single-blinded-keys-1",
      joinKeyAndIdCollection { joinKeysAndIds += singleBlindedKeysAndIds }.toByteString()
    )
  }
  private val blobOfSingleBlindedKeysWithOneMissing = runBlocking {
    mockStorage.createBlob(
      "single-blinded-keys-2",
      joinKeyAndIdCollection { joinKeysAndIds += singleBlindedKeysAndIds.drop(1) }.toByteString()
    )
  }
  private val blobOfSingleBlindedKeysAndWrongIds = runBlocking {
    mockStorage.createBlob(
      "single-blinded-keys-3",
      joinKeyAndIdCollection { joinKeysAndIds += singleBlindedKeysAndWrongIds }.toByteString()
    )
  }

  @Test
  fun `test valid intersect and validate exchange step`() = runBlocking {
    val output =
      IntersectValidateTask(maxSize = 100, minimumOverlap = 0.75f)
        .execute(
          mapOf(
            "previous-data" to blobOfSingleBlindedKeysWithOneMissing,
            "current-data" to blobOfSingleBlindedKeys
          )
        )
    assertThat(
        JoinKeyAndIdCollection.parseFrom(output.getValue("current-data").flatten())
          .joinKeysAndIdsList
      )
      .isEqualTo(singleBlindedKeysAndIds)
  }

  @Test
  fun `test data greater than max size fails to validate`() =
    runBlocking<Unit> {
      assertFailsWith(IllegalArgumentException::class) {
        IntersectValidateTask(maxSize = 1, minimumOverlap = 0.99f)
          .execute(
            mapOf(
              "previous-data" to blobOfSingleBlindedKeys,
              "current-data" to blobOfSingleBlindedKeys
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
              "previous-data" to blobOfSingleBlindedKeysWithOneMissing,
              "current-data" to blobOfSingleBlindedKeys
            )
          )
      }
    }

  @Test
  fun `test data with different ids fails to validate`() =
    runBlocking<Unit> {
      assertFailsWith(IllegalArgumentException::class) {
        IntersectValidateTask(maxSize = 100, minimumOverlap = 0.0f)
          .execute(
            mapOf(
              "previous-data" to blobOfSingleBlindedKeys,
              "current-data" to blobOfSingleBlindedKeysAndWrongIds
            )
          )
      }
    }
}
