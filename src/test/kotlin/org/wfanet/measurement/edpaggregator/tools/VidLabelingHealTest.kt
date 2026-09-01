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

package org.wfanet.measurement.edpaggregator.tools

import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.securecomputation.datawatcher.WatchedBlobs

@RunWith(JUnit4::class)
class VidLabelingHealTest {
  @Test
  fun `isAffirmative accepts y and yes case-insensitively, ignoring surrounding whitespace`() {
    assertThat(isAffirmative("yes")).isTrue()
    assertThat(isAffirmative("y")).isTrue()
    assertThat(isAffirmative("YES")).isTrue()
    assertThat(isAffirmative("Yes")).isTrue()
    assertThat(isAffirmative("  yes  ")).isTrue()
    assertThat(isAffirmative(" Y ")).isTrue()
  }

  @Test
  fun `isAffirmative rejects anything that is not exactly y or yes`() {
    assertThat(isAffirmative("no")).isFalse()
    assertThat(isAffirmative("n")).isFalse()
    assertThat(isAffirmative("yep")).isFalse()
    assertThat(isAffirmative("yesss")).isFalse()
    assertThat(isAffirmative("ye")).isFalse()
    assertThat(isAffirmative("")).isFalse()
    assertThat(isAffirmative("   ")).isFalse()
  }

  @Test
  fun `isAffirmative treats null (no stdin or EOF) as a decline`() {
    assertThat(isAffirmative(null)).isFalse()
  }

  @Test
  fun `labeled impressions prefix is validated before eviction`() {
    val parsed =
      EvictUploadsCommand.parseLabeledImpressionsBlobPrefix(
        "gs://output-bucket/reference-vid-labeled-impressions/"
      )

    assertThat(parsed.scheme).isEqualTo("gs")
    assertThat(parsed.bucket).isEqualTo("output-bucket")
    assertThat(parsed.key).isEqualTo("reference-vid-labeled-impressions")
    assertFailsWith<IllegalArgumentException> {
      EvictUploadsCommand.parseLabeledImpressionsBlobPrefix("https://output-bucket/path")
    }
  }

  @Test
  fun `rewriteDoneBlob can retry an undelivered recovery generation`() {
    val storage = mock<Storage>()
    val current = mock<Blob>()
    val created = mock<Blob>()
    val metadata =
      mapOf(
        WatchedBlobs.OVERRIDE_MODEL_LINES_KEY to "modelLines/ml1",
        WatchedBlobs.RECOVERY_SOURCE_UPLOAD_KEY to "rawImpressionUploads/up1",
      )
    whenever(storage.get(BlobId.of("bucket", "path/done"))).thenReturn(current)
    whenever(current.generation).thenReturn(11L)
    whenever(current.metadata).thenReturn(metadata)
    whenever(storage.create(any<BlobInfo>(), any<ByteArray>(), any<Storage.BlobTargetOption>()))
      .thenReturn(created)
    whenever(created.generation).thenReturn(12L)

    val generation =
      RecoverUploadCommand.rewriteDoneBlob(
        storage,
        "gs://bucket/path/done",
        expectedGeneration = 10L,
        metadata = metadata,
      )

    assertThat(generation).isEqualTo(12L)
    val blobInfo = argumentCaptor<BlobInfo>()
    val targetOption = argumentCaptor<Storage.BlobTargetOption>()
    verify(storage).create(blobInfo.capture(), any<ByteArray>(), targetOption.capture())
    assertThat(blobInfo.firstValue.metadata).containsAtLeastEntriesIn(metadata)
    assertThat(targetOption.firstValue).isEqualTo(Storage.BlobTargetOption.generationMatch(11L))
  }

  @Test
  fun `rewriteDoneBlob rejects a newer unrelated generation`() {
    val storage = mock<Storage>()
    val current = mock<Blob>()
    whenever(storage.get(BlobId.of("bucket", "path/done"))).thenReturn(current)
    whenever(current.generation).thenReturn(11L)
    whenever(current.metadata).thenReturn(emptyMap())

    val error =
      assertFailsWith<IllegalArgumentException> {
        RecoverUploadCommand.rewriteDoneBlob(
          storage,
          "gs://bucket/path/done",
          expectedGeneration = 10L,
          mapOf(WatchedBlobs.OVERRIDE_MODEL_LINES_KEY to "modelLines/ml1"),
        )
      }

    assertThat(error).hasMessageThat().contains("does not carry the same recovery metadata")
  }
}
