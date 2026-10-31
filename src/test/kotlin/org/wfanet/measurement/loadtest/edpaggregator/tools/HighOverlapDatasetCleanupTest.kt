// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.edpaggregator.tools

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.integration.common.HighOverlapDataset
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.StorageException
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class HighOverlapDatasetCleanupTest {
  @Test
  fun `prepareForReseeding removes every date and blob under selected prefixes`() =
    runBlocking<Unit> {
      val storage = InMemoryStorageClient()
      val impressionPaths = setOf(IMPRESSION_PATH, "edp/second-test-edp")
      writeBlobs(storage, listOf(MANIFEST_KEY))
      for (path in impressionPaths) {
        for (date in listOf("2026-04-01", "2026-04-02")) {
          writeBlobs(
            storage,
            listOf("impressions.binpb", "metadata.binpb", "done").map {
              "$path/model-line/$MODEL_LINE_ID/$date/$it"
            },
          )
        }
      }

      HighOverlapDatasetCleanup(storage, MODEL_LINE_ID).prepareForReseeding(impressionPaths)

      assertThat(storage.contents).isEmpty()
    }

  @Test
  fun `prepareForReseeding preserves other model lines and unrelated paths`() =
    runBlocking<Unit> {
      val storage = InMemoryStorageClient()
      val preservedKeys =
        listOf(
          "$IMPRESSION_PATH/model-line/other-line/2026-04-01/done",
          "$IMPRESSION_PATH/model-line/$MODEL_LINE_ID-other/2026-04-01/impressions.binpb",
          "edp/other-edp/model-line/$MODEL_LINE_ID/2026-04-01/done",
          "$IMPRESSION_PATH/raw-impressions/2026-04-01/done",
          "test-edp/event-groups-map/test-edp-event-group.binpb",
          HighOverlapDataset.manifestBlobKey("other-line"),
        )
      writeBlobs(storage, preservedKeys + listOf(MANIFEST_KEY, IMPRESSION_KEY))

      HighOverlapDatasetCleanup(storage, MODEL_LINE_ID).prepareForReseeding(setOf(IMPRESSION_PATH))

      assertThat(storage.contents.keys).containsExactlyElementsIn(preservedKeys)
    }

  @Test
  fun `prepareForReseeding succeeds when no previous dataset exists`() =
    runBlocking<Unit> {
      val storage = InMemoryStorageClient()

      HighOverlapDatasetCleanup(storage, MODEL_LINE_ID).prepareForReseeding(setOf(IMPRESSION_PATH))

      assertThat(storage.contents).isEmpty()
    }

  @Test
  fun `prepareForReseeding removes partial data when the manifest is already absent`() =
    runBlocking<Unit> {
      val storage = InMemoryStorageClient()
      writeBlobs(
        storage,
        listOf(IMPRESSION_KEY, "$IMPRESSION_PATH/model-line/$MODEL_LINE_ID/2026-04-01/done"),
      )

      HighOverlapDatasetCleanup(storage, MODEL_LINE_ID).prepareForReseeding(setOf(IMPRESSION_PATH))

      assertThat(storage.contents).isEmpty()
    }

  @Test
  fun `prepareForReseeding invalidates manifest before listing and propagates listing failures`() =
    runBlocking<Unit> {
      val delegate = InMemoryStorageClient()
      writeBlobs(delegate, listOf(MANIFEST_KEY, IMPRESSION_KEY))
      val failure = StorageException("Cannot list impressions")
      val storage =
        object : StorageClient by delegate {
          override suspend fun listBlobs(prefix: String?): Flow<StorageClient.Blob> {
            assertThat(delegate.getBlob(MANIFEST_KEY)).isNull()
            throw failure
          }
        }

      val exception =
        assertFailsWith<StorageException> {
          HighOverlapDatasetCleanup(storage, MODEL_LINE_ID)
            .prepareForReseeding(setOf(IMPRESSION_PATH))
        }

      assertThat(exception).isSameInstanceAs(failure)
      assertThat(delegate.contents.keys).containsExactly(IMPRESSION_KEY)
    }

  @Test
  fun `prepareForReseeding preserves impressions when manifest deletion fails`() =
    runBlocking<Unit> {
      val delegate = InMemoryStorageClient()
      writeBlobs(delegate, listOf(MANIFEST_KEY, IMPRESSION_KEY))
      val failure = StorageException("Cannot delete manifest")
      val storage =
        object : StorageClient by delegate {
          override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
            val blob = delegate.getBlob(blobKey) ?: return null
            if (blobKey != MANIFEST_KEY) {
              return blob
            }
            return object : StorageClient.Blob by blob {
              override suspend fun delete() {
                throw failure
              }
            }
          }
        }

      val exception =
        assertFailsWith<StorageException> {
          HighOverlapDatasetCleanup(storage, MODEL_LINE_ID)
            .prepareForReseeding(setOf(IMPRESSION_PATH))
        }

      assertThat(exception).isSameInstanceAs(failure)
      assertThat(delegate.contents.keys).containsExactly(MANIFEST_KEY, IMPRESSION_KEY)
    }

  @Test
  fun `prepareForReseeding propagates impression deletion failures without a manifest`() =
    runBlocking<Unit> {
      val delegate = InMemoryStorageClient()
      writeBlobs(delegate, listOf(MANIFEST_KEY, IMPRESSION_KEY))
      val failure = StorageException("Cannot delete impression")
      val storage =
        object : StorageClient by delegate {
          override suspend fun listBlobs(prefix: String?): Flow<StorageClient.Blob> {
            return delegate.listBlobs(prefix).map { blob ->
              object : StorageClient.Blob by blob {
                override suspend fun delete() {
                  throw failure
                }
              }
            }
          }
        }

      val exception =
        assertFailsWith<StorageException> {
          HighOverlapDatasetCleanup(storage, MODEL_LINE_ID)
            .prepareForReseeding(setOf(IMPRESSION_PATH))
        }

      assertThat(exception).isSameInstanceAs(failure)
      assertThat(delegate.contents.keys).containsExactly(IMPRESSION_KEY)
    }

  private suspend fun writeBlobs(storage: StorageClient, keys: Iterable<String>) {
    for (key in keys) {
      storage.writeBlob(key, ByteString.EMPTY)
    }
  }

  companion object {
    private const val MODEL_LINE_ID = "test-line"
    private const val IMPRESSION_PATH = "edp/test-edp"
    private const val IMPRESSION_KEY =
      "$IMPRESSION_PATH/model-line/$MODEL_LINE_ID/2026-04-01/impressions.binpb"
    private val MANIFEST_KEY = HighOverlapDataset.manifestBlobKey(MODEL_LINE_ID)
  }
}
