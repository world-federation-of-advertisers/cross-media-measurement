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

package org.wfanet.measurement.duchy.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.timestamp
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.internal.duchy.ContinuationTokensGrpcKt.ContinuationTokensCoroutineImplBase
import org.wfanet.measurement.internal.duchy.getContinuationTokenRequest
import org.wfanet.measurement.internal.duchy.getContinuationTokenResponse
import org.wfanet.measurement.internal.duchy.setContinuationTokenRequest
import org.wfanet.measurement.system.v1alpha.streamActiveComputationsContinuationToken

@RunWith(JUnit4::class)
abstract class ContinuationTokensServiceTest<T : ContinuationTokensCoroutineImplBase> {
  /** Instance of the service under test. */
  private lateinit var service: T

  /** Constructs the service being tested. */
  protected abstract fun newService(): T

  @Before
  fun initService() {
    service = newService()
  }

  @Test
  fun `getContinuationToken returns response with empty string when called at the first time`() =
    runBlocking {
      val response = service.getContinuationToken(getContinuationTokenRequest {})

      assertThat(response).isEqualTo(getContinuationTokenResponse { token = "" })
    }

  @Test
  fun `getContinuationToken returns response with non-empty string`() = runBlocking {
    service.setContinuationToken(setContinuationTokenRequest { this.token = TOKEN_1 })

    val response = service.getContinuationToken(getContinuationTokenRequest {})

    assertThat(response).isEqualTo(getContinuationTokenResponse { token = TOKEN_1 })
  }

  @Test
  fun `setContinuationToken creates new token entry`() = runBlocking {
    val initToken = service.getContinuationToken(getContinuationTokenRequest {}).token
    assertThat(initToken).isEmpty()

    service.setContinuationToken(setContinuationTokenRequest { this.token = TOKEN_1 })

    val updatedToken = service.getContinuationToken(getContinuationTokenRequest {}).token
    assertThat(updatedToken).isEqualTo(TOKEN_1)
  }

  @Test
  fun `setContinuationToken updates token entry`() = runBlocking {
    service.setContinuationToken(setContinuationTokenRequest { token = TOKEN_1 })
    val initToken = service.getContinuationToken(getContinuationTokenRequest {}).token
    assertThat(initToken).isEqualTo(TOKEN_1)

    service.setContinuationToken(setContinuationTokenRequest { token = TOKEN_2 })

    val updatedToken = service.getContinuationToken(getContinuationTokenRequest {}).token
    assertThat(updatedToken).isEqualTo(TOKEN_2)
  }

  @Test
  fun `setContinuationToken raise exeption when token to set is older`() = runBlocking {
    service.setContinuationToken(setContinuationTokenRequest { token = TOKEN_2 })
    val initToken = service.getContinuationToken(getContinuationTokenRequest {}).token
    assertThat(initToken).isEqualTo(TOKEN_2)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.setContinuationToken(setContinuationTokenRequest { token = TOKEN_1 })
      }
    assertThat(exception.status.code).isEqualTo(Status.FAILED_PRECONDITION.code)
  }

  companion object {
    private val TOKEN_1 = createContinuationTokenString(10L, 0L)
    private val TOKEN_2 = createContinuationTokenString(20L, 1L)
  }
}

private fun createContinuationTokenString(timeSince: Long, lastComputationId: Long): String {
  val token = streamActiveComputationsContinuationToken {
    updateTimeSince = timestamp { seconds = timeSince }
    lastSeenExternalComputationId = lastComputationId
  }
  return token.toByteArray().base64UrlEncode()
}
