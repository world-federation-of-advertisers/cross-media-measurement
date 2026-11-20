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
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.measurement.integration.k8s.testing.highOverlapDatasetCleanupState
import org.measurement.integration.k8s.testing.highOverlapDatasetManifest
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.integration.common.HighOverlapDataset
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.StorageException
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class HighOverlapDatasetCleanupTest {
  @Test
  fun `prepareForReseeding removes paths from the previous dataset absent from the new config`() =
    runBlocking<Unit> {
      val storage = InMemoryStorageClient()
      val removedPath = "edp/removed-edp"
      val renamedPath = "previous/output"
      storage.writeBlob(
        CLEANUP_STATE_KEY,
        highOverlapDatasetCleanupState { impressionPaths += listOf(removedPath, renamedPath) }
          .toByteString(),
      )
      val staleKeys =
        listOf(removedPath, renamedPath).flatMap { path ->
          listOf("impressions.binpb", "metadata.binpb", "done").map {
            "$path/model-line/$MODEL_LINE_ID/2026-04-01/$it"
          }
        }
      writeBlobs(storage, staleKeys)

      HighOverlapDatasetCleanup(storage, MODEL_LINE_ID).prepareForReseeding(setOf(IMPRESSION_PATH))

      assertThat(storage.contents.keys).containsNoneIn(staleKeys)
    }

  @Test
  fun `prepareForReseeding discovers legacy paths from the previous manifest`() =
    runBlocking<Unit> {
      val storage = InMemoryStorageClient()
      storage.writeBlob(
        MANIFEST_KEY,
        highOverlapDatasetManifest { seededEdps += "removed-edp" }.toByteString(),
      )
      val staleKey = "edp/removed-edp/model-line/$MODEL_LINE_ID/2026-04-01/done"
      writeBlobs(storage, listOf(staleKey))

      HighOverlapDatasetCleanup(storage, MODEL_LINE_ID).prepareForReseeding(setOf(IMPRESSION_PATH))

      assertThat(storage.contents.keys).doesNotContain(staleKey)
    }

  @Test
  fun `prepareForReseeding remembers partially seeded paths after config changes`() =
    runBlocking<Unit> {
      val storage = InMemoryStorageClient()
      HighOverlapDatasetCleanup(storage, MODEL_LINE_ID).prepareForReseeding(setOf(IMPRESSION_PATH))
      writeBlobs(storage, listOf(IMPRESSION_KEY))

      HighOverlapDatasetCleanup(storage, MODEL_LINE_ID).prepareForReseeding(setOf("new/output"))

      assertThat(storage.contents.keys).doesNotContain(IMPRESSION_KEY)
    }

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

      assertThat(storage.contents.keys).containsExactly(CLEANUP_STATE_KEY)
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
          "high-overlap/other-line/cleanup-state.binpb",
        )
      writeBlobs(storage, preservedKeys + listOf(MANIFEST_KEY, IMPRESSION_KEY))

      HighOverlapDatasetCleanup(storage, MODEL_LINE_ID).prepareForReseeding(setOf(IMPRESSION_PATH))

      assertThat(storage.contents.keys).containsExactlyElementsIn(preservedKeys + CLEANUP_STATE_KEY)
    }

  @Test
  fun `prepareForReseeding succeeds when no previous dataset exists`() =
    runBlocking<Unit> {
      val storage = InMemoryStorageClient()

      HighOverlapDatasetCleanup(storage, MODEL_LINE_ID).prepareForReseeding(setOf(IMPRESSION_PATH))

      assertThat(storage.contents.keys).containsExactly(CLEANUP_STATE_KEY)
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

      assertThat(storage.contents.keys).containsExactly(CLEANUP_STATE_KEY)
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
      assertThat(delegate.contents.keys).containsExactly(IMPRESSION_KEY, CLEANUP_STATE_KEY)
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
      assertThat(delegate.contents.keys)
        .containsExactly(MANIFEST_KEY, IMPRESSION_KEY, CLEANUP_STATE_KEY)
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
      assertThat(delegate.contents.keys).containsExactly(IMPRESSION_KEY, CLEANUP_STATE_KEY)

      HighOverlapDatasetCleanup(delegate, MODEL_LINE_ID).prepareForReseeding(setOf("new/output"))

      assertThat(delegate.contents.keys).containsExactly(CLEANUP_STATE_KEY)
    }

  @Test
  fun `prepareForReseeding preserves manifest and data when saving cleanup state fails`() =
    runBlocking<Unit> {
      val delegate = InMemoryStorageClient()
      writeBlobs(delegate, listOf(MANIFEST_KEY, IMPRESSION_KEY))
      val failure = StorageException("Cannot save cleanup state")
      val storage =
        object : StorageClient by delegate {
          override suspend fun writeBlob(blobKey: String, content: ByteString): StorageClient.Blob {
            throw failure
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
  fun `prepareForReseeding removes previous data when all event groups are removed`() =
    runBlocking<Unit> {
      val storage = InMemoryStorageClient()
      HighOverlapDatasetCleanup(storage, MODEL_LINE_ID).prepareForReseeding(setOf(IMPRESSION_PATH))
      writeBlobs(storage, listOf(IMPRESSION_KEY))

      HighOverlapDatasetCleanup(storage, MODEL_LINE_ID).prepareForReseeding(emptySet())

      assertThat(storage.contents.keys).containsExactly(CLEANUP_STATE_KEY)
    }

  @Test
  fun `prepareIfNeeded preserves the current manifest and impressions`() =
    runBlocking<Unit> {
      val storage = InMemoryStorageClient()
      val manifest = currentManifest.toBuilder().setImpressionRowCount(100).build()
      storage.writeBlob(MANIFEST_KEY, manifest.toByteString())
      writeBlobs(storage, listOf(IMPRESSION_KEY))

      val needsSeeding =
        HighOverlapDatasetCleanup(storage, MODEL_LINE_ID)
          .prepareIfNeeded(currentManifest, setOf(IMPRESSION_PATH))

      assertThat(needsSeeding).isFalse()
      assertThat(storage.contents.keys).containsExactly(MANIFEST_KEY, IMPRESSION_KEY)
      assertThat(storage.getBlob(MANIFEST_KEY)!!.read().flatten())
        .isEqualTo(manifest.toByteString())
    }

  @Test
  fun `prepareIfNeeded cleans a partial seed when the manifest is missing`() =
    runBlocking<Unit> {
      val storage = InMemoryStorageClient()
      HighOverlapDatasetCleanup(storage, MODEL_LINE_ID).prepareForReseeding(setOf(IMPRESSION_PATH))
      writeBlobs(storage, listOf(IMPRESSION_KEY))

      val needsSeeding =
        HighOverlapDatasetCleanup(storage, MODEL_LINE_ID)
          .prepareIfNeeded(currentManifest, setOf(IMPRESSION_PATH))

      assertThat(needsSeeding).isTrue()
      assertThat(storage.contents.keys).containsExactly(CLEANUP_STATE_KEY)
    }

  @Test
  fun `prepareIfNeeded removes old paths when specs change without touching another model line`() =
    runBlocking<Unit> {
      val storage = InMemoryStorageClient()
      val oldPath = "old/output"
      val staleKey = "$oldPath/model-line/$MODEL_LINE_ID/impressions.binpb"
      val otherLineKey = "$oldPath/model-line/other-line/impressions.binpb"
      storage.writeBlob(
        CLEANUP_STATE_KEY,
        highOverlapDatasetCleanupState { impressionPaths += oldPath }.toByteString(),
      )
      storage.writeBlob(
        MANIFEST_KEY,
        currentManifest.toBuilder().setSpecFingerprint("old-specs").build().toByteString(),
      )
      writeBlobs(storage, listOf(staleKey, otherLineKey, IMPRESSION_KEY))

      val needsSeeding =
        HighOverlapDatasetCleanup(storage, MODEL_LINE_ID)
          .prepareIfNeeded(currentManifest, setOf(IMPRESSION_PATH))

      assertThat(needsSeeding).isTrue()
      assertThat(storage.contents.keys).containsExactly(CLEANUP_STATE_KEY, otherLineKey)
    }

  @Test
  fun `prepareIfNeeded reseeds after adding or removing an EDP`() =
    runBlocking<Unit> {
      for (previousEdps in listOf(emptyList(), listOf("test-edp", "removed-edp"))) {
        val storage = InMemoryStorageClient()
        storage.writeBlob(
          MANIFEST_KEY,
          currentManifest
            .toBuilder()
            .clearSeededEdps()
            .addAllSeededEdps(previousEdps)
            .build()
            .toByteString(),
        )
        val removedKey = "edp/removed-edp/model-line/$MODEL_LINE_ID/impressions.binpb"
        if ("removed-edp" in previousEdps) writeBlobs(storage, listOf(removedKey))

        val needsSeeding =
          HighOverlapDatasetCleanup(storage, MODEL_LINE_ID)
            .prepareIfNeeded(currentManifest, setOf(IMPRESSION_PATH))

        assertThat(needsSeeding).isTrue()
        assertThat(storage.contents.keys).containsExactly(CLEANUP_STATE_KEY)
      }
    }

  @Test
  fun `prepareIfNeeded rejects a manifest for a different model line`() =
    runBlocking<Unit> {
      val storage = InMemoryStorageClient()
      storage.writeBlob(
        MANIFEST_KEY,
        currentManifest.toBuilder().setModelLine("other-model-line").build().toByteString(),
      )
      writeBlobs(storage, listOf(IMPRESSION_KEY))

      assertThat(
          HighOverlapDatasetCleanup(storage, MODEL_LINE_ID)
            .prepareIfNeeded(currentManifest, setOf(IMPRESSION_PATH))
        )
        .isTrue()
      assertThat(storage.contents.keys).containsExactly(CLEANUP_STATE_KEY)
    }

  @Test
  fun `prepareIfNeeded cleans the configured paths when the manifest is invalid`() =
    runBlocking<Unit> {
      val storage = InMemoryStorageClient()
      storage.writeBlob(MANIFEST_KEY, ByteString.copyFromUtf8("invalid"))
      writeBlobs(storage, listOf(IMPRESSION_KEY))

      val needsSeeding =
        HighOverlapDatasetCleanup(storage, MODEL_LINE_ID)
          .prepareIfNeeded(currentManifest, setOf(IMPRESSION_PATH))

      assertThat(needsSeeding).isTrue()
      assertThat(storage.contents.keys).containsExactly(CLEANUP_STATE_KEY)
    }

  @Test
  fun `prepareIfNeeded cleans previous paths from cleanup state when the manifest is truncated`() =
    runBlocking<Unit> {
      val storage = InMemoryStorageClient()
      val oldPath = "old/output"
      val staleKey = "$oldPath/model-line/$MODEL_LINE_ID/impressions.binpb"
      val otherLineKey = "$oldPath/model-line/other-line/impressions.binpb"
      storage.writeBlob(
        CLEANUP_STATE_KEY,
        highOverlapDatasetCleanupState { impressionPaths += oldPath }.toByteString(),
      )
      val manifestBytes = currentManifest.toByteString()
      storage.writeBlob(MANIFEST_KEY, manifestBytes.substring(0, manifestBytes.size() - 1))
      writeBlobs(storage, listOf(staleKey, otherLineKey, IMPRESSION_KEY))

      val needsSeeding =
        HighOverlapDatasetCleanup(storage, MODEL_LINE_ID)
          .prepareIfNeeded(currentManifest, setOf(IMPRESSION_PATH))

      assertThat(needsSeeding).isTrue()
      assertThat(storage.contents.keys).containsExactly(CLEANUP_STATE_KEY, otherLineKey)
    }

  @Test
  fun `prepareIfNeeded propagates manifest read errors without deleting data`() =
    runBlocking<Unit> {
      val delegate = InMemoryStorageClient()
      delegate.writeBlob(MANIFEST_KEY, currentManifest.toByteString())
      writeBlobs(delegate, listOf(IMPRESSION_KEY))
      val failure = StorageException("Cannot read manifest")
      val storage =
        object : StorageClient by delegate {
          override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
            throw failure
          }
        }

      val exception =
        assertFailsWith<StorageException> {
          HighOverlapDatasetCleanup(storage, MODEL_LINE_ID)
            .prepareIfNeeded(currentManifest, setOf(IMPRESSION_PATH))
        }

      assertThat(exception).isSameInstanceAs(failure)
      assertThat(delegate.contents.keys).containsExactly(MANIFEST_KEY, IMPRESSION_KEY)
    }

  @Test
  fun `prepareIfNeeded propagates blob read errors without deleting data`() =
    runBlocking<Unit> {
      val delegate = InMemoryStorageClient()
      delegate.writeBlob(MANIFEST_KEY, currentManifest.toByteString())
      writeBlobs(delegate, listOf(IMPRESSION_KEY))
      val failure = StorageException("Cannot read manifest bytes")
      val storage =
        object : StorageClient by delegate {
          override suspend fun getBlob(blobKey: String): StorageClient.Blob? {
            val blob = delegate.getBlob(blobKey) ?: return null
            if (blobKey != MANIFEST_KEY) return blob
            return object : StorageClient.Blob by blob {
              override fun read(): Flow<ByteString> = flow { throw failure }
            }
          }
        }

      val exception =
        assertFailsWith<StorageException> {
          HighOverlapDatasetCleanup(storage, MODEL_LINE_ID)
            .prepareIfNeeded(currentManifest, setOf(IMPRESSION_PATH))
        }

      assertThat(exception).isSameInstanceAs(failure)
      assertThat(delegate.contents.keys).containsExactly(MANIFEST_KEY, IMPRESSION_KEY)
    }

  private suspend fun writeBlobs(storage: StorageClient, keys: Iterable<String>) {
    for (key in keys) {
      storage.writeBlob(key, ByteString.EMPTY)
    }
  }

  private val currentManifest = highOverlapDatasetManifest {
    modelLine = "modelProviders/provider/modelSuites/suite/modelLines/$MODEL_LINE_ID"
    specFingerprint = "current-specs"
    seededEdps += "test-edp"
  }

  companion object {
    private const val MODEL_LINE_ID = "test-line"
    private const val IMPRESSION_PATH = "edp/test-edp"
    private const val IMPRESSION_KEY =
      "$IMPRESSION_PATH/model-line/$MODEL_LINE_ID/2026-04-01/impressions.binpb"
    private val MANIFEST_KEY = HighOverlapDataset.manifestBlobKey(MODEL_LINE_ID)
    private const val CLEANUP_STATE_KEY = "high-overlap/$MODEL_LINE_ID/cleanup-state.binpb"
  }
}
