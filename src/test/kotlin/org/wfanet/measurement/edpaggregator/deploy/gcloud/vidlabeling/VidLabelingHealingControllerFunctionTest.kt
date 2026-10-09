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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.vidlabeling

import com.google.api.gax.paging.Page
import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.common.truth.Truth.assertThat
import com.google.type.date
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.edpaggregator.vidlabeling.RawImpressionUploadManifestClassifier
import org.wfanet.measurement.edpaggregator.vidlabeling.healing.DoneBlobReplayer
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcher
import org.wfanet.measurement.securecomputation.datawatcher.WatchedBlobs

@RunWith(JUnit4::class)
class VidLabelingHealingControllerFunctionTest {
  @Test
  fun `service runs controller and returns no content`() {
    val invoked = CompletableDeferred<Unit>()
    val response = mock<com.google.cloud.functions.HttpResponse>()
    val function = VidLabelingHealingControllerFunction { invoked.complete(Unit) }

    function.service(mock(), response)

    assertThat(invoked.isCompleted).isTrue()
    verify(response).setStatusCode(204)
  }

  @Test
  fun `manifest reader returns live objects for exact done generation`() {
    runBlocking {
      val storage = mock<Storage>()
      val done = blob("day/done", 11L)
      val first = blob("day/first.avro", 21L)
      val second = blob("day/second.avro", 22L)
      val nestedDone = blob("day/backfill/DONE", 23L)
      val page = mock<Page<Blob>>()
      whenever(storage.get("raw", "day/done")).thenReturn(done)
      whenever(storage.list(eq("raw"), any<Storage.BlobListOption>())).thenReturn(page)
      whenever(page.iterateAll()).thenReturn(listOf(first, done, second, nestedDone))

      val manifest =
        GcsCorrectionManifestReader(storage).read("gs://raw/day/done", 11L, emptyList())

      assertThat(manifest)
        .containsExactly(
          RawImpressionUploadManifestClassifier.File("gs://raw/day/first.avro", 21L),
          RawImpressionUploadManifestClassifier.File("gs://raw/day/second.avro", 22L),
        )
    }
  }

  @Test
  fun `manifest reader preserves persisted dates for exact object generations`() = runBlocking {
    val storage = mock<Storage>()
    val done = blob("day/done", 11L)
    val input = blob("day/input.parquet", 21L)
    val page = mock<Page<Blob>>()
    whenever(storage.get("raw", "day/done")).thenReturn(done)
    whenever(storage.list(eq("raw"), any<Storage.BlobListOption>())).thenReturn(page)
    whenever(page.iterateAll()).thenReturn(listOf(input, done))
    val persisted =
      listOf(
        RawImpressionUploadManifestClassifier.File(
          "gs://raw/day/input.parquet",
          21L,
          date {
            year = 2026
            month = 9
            day = 1
          },
        )
      )

    val live = GcsCorrectionManifestReader(storage).read("gs://raw/day/done", 11L, persisted)

    val classifier = RawImpressionUploadManifestClassifier()
    assertThat(live).containsExactlyElementsIn(persisted)
    assertThat(classifier.digest(live)).isEqualTo(classifier.digest(persisted))
  }

  @Test
  fun `manifest reader rejects changed done generation`() {
    val storage = mock<Storage>()
    val done = blob("day/done", 12L)
    whenever(storage.get("raw", "day/done")).thenReturn(done)

    val error =
      kotlin.test.assertFailsWith<IllegalStateException> {
        runBlocking {
          GcsCorrectionManifestReader(storage).read("gs://raw/day/done", 11L, emptyList())
        }
      }

    assertThat(error).hasMessageThat().contains("generation changed")
  }

  @Test
  fun `manifest reader rejects done rewrite during listing`() {
    val storage = mock<Storage>()
    val original = blob("day/done", 11L)
    val rewritten = blob("day/done", 12L)
    val page = mock<Page<Blob>>()
    whenever(storage.get("raw", "day/done")).thenReturn(original, rewritten)
    whenever(storage.list(eq("raw"), any<Storage.BlobListOption>())).thenReturn(page)
    whenever(page.iterateAll()).thenReturn(emptyList())

    val error =
      kotlin.test.assertFailsWith<IllegalStateException> {
        runBlocking {
          GcsCorrectionManifestReader(storage).read("gs://raw/day/done", 11L, emptyList())
        }
      }

    assertThat(error).hasMessageThat().contains("generation changed")
  }

  @Test
  fun `labeled output store deletes only the requested generation`() {
    val storage = mock<Storage>()
    val live = blob("day/output.avro", 42L)
    val unqualified = BlobId.of("output", "day/output.avro")
    val qualified = BlobId.of("output", "day/output.avro", 42L)
    whenever(storage.get(unqualified)).thenReturn(live)
    whenever(storage.delete(qualified)).thenReturn(true)
    val outputStore = GcsLabeledOutputStore(storage)

    assertThat(outputStore.getGeneration("gs://output/day/output.avro")).isEqualTo(42L)
    assertThat(outputStore.delete("gs://output/day/output.avro", 42L)).isTrue()

    verify(storage).delete(qualified)
    verify(storage, never()).delete(unqualified)
  }

  @Test
  fun `recovery writer creates a fresh generation with recovery metadata`() {
    val storage = mock<Storage>()
    val current = blob("day/done", 11L)
    val rewritten = blob("day/done", 12L)
    whenever(current.metadata).thenReturn(mapOf("existing" to "value"))
    whenever(storage.get(BlobId.of("raw", "day/done"))).thenReturn(current)
    whenever(storage.create(any<BlobInfo>(), any<ByteArray>(), any<Storage.BlobTargetOption>()))
      .thenReturn(rewritten)

    val generation =
      GcsDoneBlobRecoveryWriter(storage)
        .rewrite("gs://raw/day/done", 11L, mapOf("recovery" to "operation"))

    assertThat(generation).isEqualTo(12L)
    val blobInfo = argumentCaptor<BlobInfo>()
    verify(storage).create(blobInfo.capture(), any<ByteArray>(), any<Storage.BlobTargetOption>())
    assertThat(blobInfo.firstValue.metadata)
      .containsExactly("existing", "value", "recovery", "operation")
  }

  @Test
  fun `recovery writer reuses a matching recovery generation`() {
    val storage = mock<Storage>()
    val current = blob("day/done", 12L)
    whenever(current.metadata).thenReturn(mapOf("recovery" to "operation"))
    whenever(storage.get(BlobId.of("raw", "day/done"))).thenReturn(current)

    val generation =
      GcsDoneBlobRecoveryWriter(storage)
        .rewrite("gs://raw/day/done", 11L, mapOf("recovery" to "operation"))

    assertThat(generation).isEqualTo(12L)
    verify(storage, never())
      .create(any<BlobInfo>(), any<ByteArray>(), any<Storage.BlobTargetOption>())
  }

  @Test
  fun `done replayer routes exact recovery metadata`() {
    runBlocking {
      val dataWatcher = mock<DataWatcher>()
      val request =
        DoneBlobReplayer.Request(
          doneBlobUri = "gs://raw/day/done",
          doneBlobGeneration = 11L,
          sourceRawImpressionUpload = "dataProviders/dp/rawImpressionUploads/source",
          cmmsModelLines = listOf("modelLines/first", "modelLines/second"),
          uploadHealingOperation =
            "dataProviders/dp/uploadHealingOperations/bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        )

      DataWatcherDoneBlobReplayer(dataWatcher).replay(request)

      verify(dataWatcher)
        .receivePath(
          "gs://raw/day/done",
          mapOf(
            DataWatcher.GENERATION_METADATA_KEY to "11",
            WatchedBlobs.OVERRIDE_MODEL_LINES_KEY to "modelLines/first,modelLines/second",
            WatchedBlobs.RECOVERY_SOURCE_UPLOAD_KEY to
              "dataProviders/dp/rawImpressionUploads/source",
            WatchedBlobs.EVICTION_OPERATION_ID_KEY to "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
          ),
        )
    }
  }

  private fun blob(name: String, generation: Long): Blob {
    return mock<Blob>().also {
      whenever(it.name).thenReturn(name)
      whenever(it.generation).thenReturn(generation)
    }
  }
}
