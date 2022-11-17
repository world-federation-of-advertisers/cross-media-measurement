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

package org.wfanet.measurement.duchy.deploy.gcloud.spanner.continuationtoken

import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.internal.duchy.getContinuationTokenRequest
import org.wfanet.measurement.internal.duchy.getContinuationTokenResponse
import org.wfanet.measurement.internal.duchy.updateContinuationTokenRequest

@RunWith(JUnit4::class)
class SpannerContinuationTokensServiceTest {
  @get:Rule val spannerDatabase = SpannerEmulatorDatabaseRule(Schemata.DUCHY_CHANGELOG_PATH)
  private lateinit var continuationTokensService: SpannerContinuationTokensService

  @Before
  fun init() {
    continuationTokensService = SpannerContinuationTokensService(spannerDatabase.databaseClient)
  }

  @Test
  fun `getContinuationToken returns response with empty string when called at the first time`() =
    runBlocking {
      val response = continuationTokensService.getContinuationToken(getContinuationTokenRequest {})

      assertThat(response).isEqualTo(getContinuationTokenResponse { token = "" })
    }

  @Test
  fun `getContinuationToken returns response with non-empty string`() = runBlocking {
    continuationTokensService.updateContinuationToken(
      updateContinuationTokenRequest { this.token = "token1" }
    )

    val response = continuationTokensService.getContinuationToken(getContinuationTokenRequest {})

    assertThat(response).isEqualTo(getContinuationTokenResponse { token = "token1" })
  }

  @Test
  fun `updateContinuationToken creates new token entry`() = runBlocking {
    val initToken =
      continuationTokensService.getContinuationToken(getContinuationTokenRequest {}).token
    assertThat(initToken).isEmpty()

    continuationTokensService.updateContinuationToken(
      updateContinuationTokenRequest { this.token = "updated_token" }
    )

    val updatedToken =
      continuationTokensService.getContinuationToken(getContinuationTokenRequest {}).token
    assertThat(updatedToken).isEqualTo("updated_token")
  }

  @Test
  fun `updateContinuationToken updates token entry`() = runBlocking {
    continuationTokensService.updateContinuationToken(
      updateContinuationTokenRequest { token = "token1" }
    )
    val initToken =
      continuationTokensService.getContinuationToken(getContinuationTokenRequest {}).token
    assertThat(initToken).isEqualTo("token1")

    continuationTokensService.updateContinuationToken(
      updateContinuationTokenRequest { token = "token2" }
    )

    val updatedToken =
      continuationTokensService.getContinuationToken(getContinuationTokenRequest {}).token
    assertThat(updatedToken).isEqualTo("token2")
  }
}
