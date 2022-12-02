// Copyright 2022 The Cross-Media Measurement Authors
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
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.testing.GrpcCleanupRule
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.duchy.service.internal.testing.InMemoryContinuationTokensService
import org.wfanet.measurement.internal.duchy.ContinuationTokensGrpcKt.ContinuationTokensCoroutineStub

@RunWith(JUnit4::class)
class ContinuationTokenManagerTest {
  private val grpcCleanup = GrpcCleanupRule()

  private lateinit var continuationTokenManager: ContinuationTokenManager

  private lateinit var inMemoryContinuationTokensService: InMemoryContinuationTokensService

  @Before
  fun init() {
    inMemoryContinuationTokensService = InMemoryContinuationTokensService()

    val serverName: String = InProcessServerBuilder.generateName()
    grpcCleanup.register(
      InProcessServerBuilder.forName(serverName)
        .directExecutor()
        .addService(inMemoryContinuationTokensService)
        .build()
        .start()
    )
    val channel =
      grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build())
    val client = ContinuationTokensCoroutineStub(channel)
    continuationTokenManager = ContinuationTokenManager(client)
  }

  @Test
  fun `getLatestContinuationToken returns empty string when called the first time`() = runBlocking {
    val token = continuationTokenManager.getLatestContinuationToken()

    assertThat(token).isEmpty()
  }

  @Test
  fun `getLatestContinuationToken returns the last token when all tokens are processed`() =
    runBlocking {
      continuationTokenManager.addPendingToken("1")
      continuationTokenManager.addPendingToken("2")
      continuationTokenManager.markTokenProcessed("1")
      continuationTokenManager.addPendingToken("3")
      continuationTokenManager.markTokenProcessed("2")
      continuationTokenManager.markTokenProcessed("3")

      assertThat(continuationTokenManager.getLatestContinuationToken()).isEqualTo("3")
      assertThat(inMemoryContinuationTokensService.latestContinuationToken).isEqualTo("3")
    }

  @Test
  fun `getLatestContinuationToken returns token when the last token is marked as processed`() =
    runBlocking {
      continuationTokenManager.addPendingToken("1")
      continuationTokenManager.addPendingToken("2")
      continuationTokenManager.markTokenProcessed("1")
      continuationTokenManager.addPendingToken("3")
      continuationTokenManager.markTokenProcessed("3")

      assertThat(continuationTokenManager.getLatestContinuationToken()).isEqualTo("1")
      assertThat(inMemoryContinuationTokensService.latestContinuationToken).isEqualTo("1")
    }

  @Test
  fun `getLatestContinuationToken returns  token when token 4 are marked as processed`() =
    runBlocking {
      continuationTokenManager.getLatestContinuationToken()
      continuationTokenManager.addPendingToken("1")
      continuationTokenManager.markTokenProcessed("1")
      continuationTokenManager.addPendingToken("2")
      continuationTokenManager.markTokenProcessed("2")
      continuationTokenManager.addPendingToken("3")
      continuationTokenManager.markTokenProcessed("3")
      continuationTokenManager.addPendingToken("4")
      continuationTokenManager.addPendingToken("5")
      continuationTokenManager.markTokenProcessed("5")
      continuationTokenManager.addPendingToken("6")
      continuationTokenManager.markTokenProcessed("6")
      continuationTokenManager.addPendingToken("7")
      continuationTokenManager.addPendingToken("8")

      assertThat(continuationTokenManager.getLatestContinuationToken()).isEqualTo("3")

      continuationTokenManager.markTokenProcessed("4")
      assertThat(continuationTokenManager.getLatestContinuationToken()).isEqualTo("6")
      assertThat(inMemoryContinuationTokensService.latestContinuationToken).isEqualTo("6")
    }
}
