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

package org.wfanet.measurement.duchy.daemon.herald

import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.duchy.daemon.herald.testing.InMemoryContinuationTokenStore

private const val DUCHY_NAME = "worker"
private const val CONTINUATION_TOKEN_1 = "token1"
private const val CONTINUATION_TOKEN_2 = "token2"
private const val CONTINUATION_TOKEN_3 = "token3"

@RunWith(JUnit4::class)
class ContinuationTokenManagerTest {
  private lateinit var continuationTokenManager: ContinuationTokenManager

  @Before
  fun init() {
    val continuationTokenStore = InMemoryContinuationTokenStore()
    continuationTokenManager = ContinuationTokenManager(DUCHY_NAME, continuationTokenStore)
  }

  @Test
  fun `getLatestContinuationToken returns empty string when called the first time`() = runBlocking {
    val token = continuationTokenManager.getLatestContinuationToken()

    assertThat(token).isEmpty()
  }

  @Test
  fun `getLatestContinuationToken returns the latest token when all computation processed`() =
    runBlocking {
      continuationTokenManager.getLatestContinuationToken()
      val index1 = continuationTokenManager.addContinuationToken(CONTINUATION_TOKEN_1)
      val index2 = continuationTokenManager.addContinuationToken(CONTINUATION_TOKEN_2)
      continuationTokenManager.updateContinuationToken(index1)
      val index3 = continuationTokenManager.addContinuationToken(CONTINUATION_TOKEN_3)
      continuationTokenManager.updateContinuationToken(index2)
      continuationTokenManager.updateContinuationToken(index3)

      val token = continuationTokenManager.getLatestContinuationToken()

      assertThat(token).isEqualTo(CONTINUATION_TOKEN_3)
    }

  @Test
  fun `getLatestContinuationToken returns processed token`() = runBlocking {
    continuationTokenManager.getLatestContinuationToken()
    val index1 = continuationTokenManager.addContinuationToken(CONTINUATION_TOKEN_1)
    val index2 = continuationTokenManager.addContinuationToken(CONTINUATION_TOKEN_2)
    continuationTokenManager.updateContinuationToken(index1)
    val index3 = continuationTokenManager.addContinuationToken(CONTINUATION_TOKEN_3)
    continuationTokenManager.updateContinuationToken(index3)

    val token = continuationTokenManager.getLatestContinuationToken()

    assertThat(token).isEqualTo(CONTINUATION_TOKEN_1)
  }
}
