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

private const val DUCHY_NAME = "worker1"

@RunWith(JUnit4::class)
class SpannerContinuationTokensTest {
  @get:Rule val spannerDatabase = SpannerEmulatorDatabaseRule(Schemata.DUCHY_CHANGELOG_PATH)
  private lateinit var continuationTokens: SpannerContinuationTokens

  @Before
  fun init() {
    continuationTokens = SpannerContinuationTokens(spannerDatabase.databaseClient)
  }

  @Test
  fun `readContinuationToken returns empty string when called at the first time`() = runBlocking {
    val token = continuationTokens.readContinuationToken(DUCHY_NAME)

    assertThat(token).isEmpty()
  }

  @Test
  fun `updateContinuationToken creates new token entry`() = runBlocking {
    val emptyToken = continuationTokens.readContinuationToken(DUCHY_NAME)
    assertThat(emptyToken).isEmpty()

    continuationTokens.updateContinuationToken(DUCHY_NAME, "token1")
    val token = continuationTokens.readContinuationToken(DUCHY_NAME)
    assertThat(token).isEqualTo("token1")
  }

  @Test
  fun `updateContinuationToken updates token entry`() = runBlocking {
    continuationTokens.updateContinuationToken(DUCHY_NAME, "token1")
    val initToken = continuationTokens.readContinuationToken(DUCHY_NAME)
    assertThat(initToken).isEqualTo("token1")

    continuationTokens.updateContinuationToken(DUCHY_NAME, "token2")
    val token = continuationTokens.readContinuationToken(DUCHY_NAME)
    assertThat(token).isEqualTo("token2")
  }
}
