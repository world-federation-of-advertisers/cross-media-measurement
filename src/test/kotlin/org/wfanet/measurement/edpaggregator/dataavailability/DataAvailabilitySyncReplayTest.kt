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

package org.wfanet.measurement.edpaggregator.dataavailability

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.storage.BlobUri
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class DataAvailabilitySyncReplayTest {
  @Test
  fun `replay invokes sync for every completion blob under prefix in order`() = runBlocking {
    val storageClient = InMemoryStorageClient()
    storageClient.writeBlob("edp/meta/model-line/2/2026-01-02/done", ByteString.EMPTY)
    storageClient.writeBlob("edp/meta/model-line/1/2026-01-01/done", ByteString.EMPTY)
    storageClient.writeBlob(
      "edp/meta/model-line/1/2026-01-01/metadata.binpb",
      ByteString.copyFromUtf8("metadata"),
    )
    storageClient.writeBlob("edp/other/model-line/1/2026-01-01/done", ByteString.EMPTY)
    val replayedUris = mutableListOf<String>()
    val replay =
      DataAvailabilitySyncReplay(
        storageClient = storageClient,
        storageRootUri = BlobUri(scheme = "gs", bucket = "bucket", key = ""),
        selection = DataAvailabilitySyncReplay.Selection.BlobPrefix(value = "edp/meta"),
        sync = { doneBlobUri ->
          replayedUris += doneBlobUri
          DataAvailabilitySync.Outcome.PUBLISHED
        },
      )

    val result = replay.replay()

    assertThat(replayedUris)
      .containsExactly(
        "gs://bucket/edp/meta/model-line/1/2026-01-01/done",
        "gs://bucket/edp/meta/model-line/2/2026-01-02/done",
      )
      .inOrder()
    assertThat(result.doneBlobsSelected).isEqualTo(2)
    assertThat(result.published).isEqualTo(2)
    assertThat(result.noWork).isEqualTo(0)
    assertThat(result.blockedByGaps).isEqualTo(0)
    assertThat(result.errors).isEmpty()
  }

  @Test
  fun `replay reports every outcome and continues after a failure`() =
    runBlocking<Unit> {
      val storageClient = InMemoryStorageClient()
      for (date in 1..4) {
        storageClient.writeBlob("edp/meta/model-line/1/2026-01-0$date/done", ByteString.EMPTY)
      }
      var invocation = 0
      val replay =
        DataAvailabilitySyncReplay(
          storageClient = storageClient,
          storageRootUri = BlobUri(scheme = "gs", bucket = "bucket", key = ""),
          selection = DataAvailabilitySyncReplay.Selection.BlobPrefix(value = "edp/meta/"),
          sync = {
            when (invocation++) {
              0 -> DataAvailabilitySync.Outcome.NO_WORK
              1 -> error("RPC failed")
              2 -> DataAvailabilitySync.Outcome.BLOCKED_GAPS
              else -> DataAvailabilitySync.Outcome.PUBLISHED
            }
          },
        )

      val result = replay.replay()

      assertThat(result.doneBlobsSelected).isEqualTo(4)
      assertThat(result.published).isEqualTo(1)
      assertThat(result.noWork).isEqualTo(1)
      assertThat(result.blockedByGaps).isEqualTo(1)
      assertThat(result.errors)
        .containsExactly(
          DataAvailabilitySyncReplay.ReplayError(
            doneBlobUri = "gs://bucket/edp/meta/model-line/1/2026-01-02/done",
            message = "RPC failed",
          )
        )
    }

  @Test
  fun `replay returns no work when prefix has no completion blobs`() = runBlocking {
    val storageClient = InMemoryStorageClient()
    storageClient.writeBlob(
      "edp/meta/model-line/1/2026-01-01/metadata.binpb",
      ByteString.copyFromUtf8("metadata"),
    )
    val replay =
      DataAvailabilitySyncReplay(
        storageClient = storageClient,
        storageRootUri = BlobUri(scheme = "gs", bucket = "bucket", key = ""),
        selection = DataAvailabilitySyncReplay.Selection.BlobPrefix(value = "edp/meta"),
        sync = { error("sync must not be invoked") },
      )

    val result = replay.replay()

    assertThat(result.doneBlobsSelected).isEqualTo(0)
    assertThat(result.published).isEqualTo(0)
    assertThat(result.noWork).isEqualTo(0)
    assertThat(result.blockedByGaps).isEqualTo(0)
    assertThat(result.errors).isEmpty()
  }

  @Test
  fun `replay invokes sync for explicitly supplied completion blobs`() = runBlocking {
    val replayedUris = mutableListOf<String>()
    val replay =
      DataAvailabilitySyncReplay(
        storageClient = InMemoryStorageClient(),
        storageRootUri = BlobUri(scheme = "gs", bucket = "bucket", key = ""),
        selection =
          DataAvailabilitySyncReplay.Selection.DoneBlobUris(
            values =
              setOf(
                "gs://bucket/edp/meta/model-line/2/2026-01-02/done",
                "gs://bucket/edp/meta/model-line/1/2026-01-01/done",
              )
          ),
        sync = { doneBlobUri ->
          replayedUris += doneBlobUri
          DataAvailabilitySync.Outcome.PUBLISHED
        },
      )

    val result = replay.replay()

    assertThat(replayedUris)
      .containsExactly(
        "gs://bucket/edp/meta/model-line/1/2026-01-01/done",
        "gs://bucket/edp/meta/model-line/2/2026-01-02/done",
      )
      .inOrder()
    assertThat(result.doneBlobsSelected).isEqualTo(2)
    assertThat(result.published).isEqualTo(2)
  }
}
