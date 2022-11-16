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
import org.wfanet.measurement.duchy.db.continuationtoken.testing.TestContinuationTokens
import org.wfanet.measurement.internal.duchy.ContinuationTokensGrpcKt.ContinuationTokensCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ContinuationTokensGrpcKt.ContinuationTokensCoroutineStub
import org.wfanet.measurement.internal.duchy.GetContinuationTokenRequest
import org.wfanet.measurement.internal.duchy.GetContinuationTokenResponse
import org.wfanet.measurement.internal.duchy.UpdateContinuationTokenRequest
import org.wfanet.measurement.internal.duchy.UpdateContinuationTokenResponse
import org.wfanet.measurement.internal.duchy.getContinuationTokenResponse

private const val DUCHY_NAME = "worker"
private const val CONTINUATION_TOKEN_1 = "token1"
private const val CONTINUATION_TOKEN_2 = "token2"
private const val CONTINUATION_TOKEN_3 = "token3"

@RunWith(JUnit4::class)
class ContinuationTokenManagerTest {
  private val grpcCleanup = GrpcCleanupRule()

  private lateinit var continuationTokenManager: ContinuationTokenManager

  @Before
  fun init() {
    // ContinuationToken service
    val serverName: String = InProcessServerBuilder.generateName()
    grpcCleanup.register(
      InProcessServerBuilder.forName(serverName)
        .directExecutor()
        .addService(TestContinuationTokensService())
        .build()
        .start()
    )
    val channel =
      grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build())
    val client = ContinuationTokensCoroutineStub(channel)
    continuationTokenManager = ContinuationTokenManager(DUCHY_NAME, client)
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

class TestContinuationTokensService : ContinuationTokensCoroutineImplBase() {
  val tokens = TestContinuationTokens()

  override suspend fun getContinuationToken(
    request: GetContinuationTokenRequest
  ): GetContinuationTokenResponse {
    return getContinuationTokenResponse { token = tokens.readContinuationToken(request.name) }
  }

  override suspend fun updateContinuationToken(
    request: UpdateContinuationTokenRequest
  ): UpdateContinuationTokenResponse {
    tokens.updateContinuationToken(request.name, request.token)
    return UpdateContinuationTokenResponse.getDefaultInstance()
  }
}
