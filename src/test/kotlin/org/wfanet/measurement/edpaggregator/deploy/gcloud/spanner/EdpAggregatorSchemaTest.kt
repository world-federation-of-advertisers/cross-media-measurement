/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner

import com.google.cloud.spanner.Statement
import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.gcloud.spanner.testing.UsingSpannerEmulator

@RunWith(JUnit4::class)
class EdpAggregatorSchemaTest : UsingSpannerEmulator(Schemata.EDP_AGGREGATOR_CHANGELOG_PATH) {

  @Test
  fun `database resolves correction candidate proto`() = runBlocking {
    val row =
      databaseClient.singleUse().use { transaction ->
        transaction
          .executeQuery(
            Statement.of(
              """
              SELECT candidate.data_provider_resource_id AS DataProviderResourceId
              FROM (
                SELECT CAST(B'' AS
                  `wfa.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidate`)
                  AS candidate
              )
              """
                .trimIndent()
            )
          )
          .single()
      }
    assertThat(row.getString("DataProviderResourceId")).isEmpty()
  }

  @Test
  fun `database is created`() {
    // No-op. Just ensure that DB is created by test infra.
  }
}
