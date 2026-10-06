/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Value
import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.VidLabelingEvictionFenceState

@RunWith(JUnit4::class)
class VidLabelingEvictionFenceTest {
  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  @Test
  fun `insert defaults to evicting`() =
    runBlocking<Unit> {
      spannerDatabase.databaseClient.readWriteTransaction().run { txn ->
        txn.insertVidLabelingEvictionFence(DATA_PROVIDER_ID, OPERATION_ID)
      }

      val fence =
        spannerDatabase.databaseClient.singleUse().use { txn ->
          txn.getVidLabelingEvictionFence(DATA_PROVIDER_ID)
        }

      assertThat(fence)
        .isEqualTo(
          VidLabelingEvictionFence(
            OPERATION_ID,
            VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_EVICTING,
          )
        )
    }

  @Test
  fun `state can advance`() =
    runBlocking<Unit> {
      spannerDatabase.databaseClient.readWriteTransaction().run { txn ->
        txn.insertVidLabelingEvictionFence(
          DATA_PROVIDER_ID,
          OPERATION_ID,
          VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_APPROVAL_PENDING,
        )
      }

      val pendingFence =
        spannerDatabase.databaseClient.singleUse().use { txn ->
          txn.getVidLabelingEvictionFence(DATA_PROVIDER_ID)
        }
      assertThat(pendingFence?.state)
        .isEqualTo(VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_APPROVAL_PENDING)

      spannerDatabase.databaseClient.readWriteTransaction().run { txn ->
        txn.updateVidLabelingEvictionFenceState(
          DATA_PROVIDER_ID,
          VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_DRAINING,
        )
      }

      val fence =
        spannerDatabase.databaseClient.singleUse().use { txn ->
          txn.getVidLabelingEvictionFence(DATA_PROVIDER_ID)
        }
      assertThat(fence?.state)
        .isEqualTo(VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_DRAINING)
    }

  @Test
  fun `legacy row defaults to evicting`() =
    runBlocking<Unit> {
      spannerDatabase.databaseClient.write(
        listOf(
          Mutation.newInsertBuilder("VidLabelingEvictionFence")
            .set("DataProviderResourceId")
            .to(DATA_PROVIDER_ID)
            .set("EvictionOperationId")
            .to(OPERATION_ID)
            .set("CreateTime")
            .to(Value.COMMIT_TIMESTAMP)
            .build()
        )
      )

      val fence =
        spannerDatabase.databaseClient.singleUse().use { txn ->
          txn.getVidLabelingEvictionFence(DATA_PROVIDER_ID)
        }
      assertThat(fence?.state)
        .isEqualTo(VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_EVICTING)
    }

  companion object {
    @JvmField @ClassRule val spannerEmulator = SpannerEmulatorRule()

    private const val DATA_PROVIDER_ID = "data-provider"
    private const val OPERATION_ID = "11111111-1111-4111-8111-111111111111"
  }
}
