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

package org.wfanet.measurement.integration.testing

import com.google.api.gax.paging.Page
import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageException
import com.google.common.truth.Truth.assertThat
import java.time.LocalDate
import kotlin.test.assertFailsWith
import org.junit.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever

class ManagedDoneBlobCleanupTest {
  private val storage: Storage = mock()
  private val deleted = mutableListOf<BlobId>()

  @Test
  fun `cleanup removes old managed markers only within the fixture dates and EDP`() {
    val marker = "done-${"a".repeat(64)}"
    val first = "$PREFIX/model-line/model/2021-03-21/$marker"
    val second = "$PREFIX/model-line/other-model/2021-03-21/$marker"
    listBlobs(
      first,
      second,
      "$PREFIX/model-line/model/2021-03-21/done",
      "$PREFIX/model-line/model/2021-03-20/$marker",
      "vid-labeled-impressions/edp/other/model-line/model/2021-03-21/$marker",
      "$PREFIX-other/model-line/model/2021-03-21/$marker",
      "$PREFIX/model-line/model/2021-03-21/nested/$marker",
      "$PREFIX/model-line/model/2021-03-21/$marker.metadata.binpb",
      "$PREFIX/model-line/model/2021-03-21/done-not-a-managed-hash",
    )
    whenever(storage.delete(any<BlobId>(), any<Storage.BlobSourceOption>())).thenAnswer {
      deleted += it.getArgument<BlobId>(0)
      assertThat(it.getArgument<Storage.BlobSourceOption>(1))
        .isEqualTo(Storage.BlobSourceOption.generationMatch(GENERATION))
      true
    }

    ManagedDoneBlobCleanup(storage, BUCKET, "$PREFIX/", setOf(DATE)).delete()

    assertThat(deleted).containsExactly(BlobId.of(BUCKET, first), BlobId.of(BUCKET, second))
  }

  @Test
  fun `cleanup fails if a managed marker is rewritten after listing`() {
    val name = "$PREFIX/model-line/model/2021-03-21/done-${"a".repeat(64)}"
    listBlobs(name)
    val failure = StorageException(412, "Generation changed")
    whenever(
        storage.delete(
          BlobId.of(BUCKET, name),
          Storage.BlobSourceOption.generationMatch(GENERATION),
        )
      )
      .thenThrow(failure)

    assertThat(
        assertFailsWith<StorageException> {
          ManagedDoneBlobCleanup(storage, BUCKET, PREFIX, setOf(DATE)).delete()
        }
      )
      .isSameInstanceAs(failure)
  }

  private fun listBlobs(vararg names: String) {
    val blobs =
      names.map { name ->
        mock<Blob> {
          on { this.name } doReturn name
          on { blobId } doReturn BlobId.of(BUCKET, name, GENERATION)
          on { generation } doReturn GENERATION
        }
      }
    val page: Page<Blob> = mock { on { iterateAll() } doReturn blobs }
    whenever(storage.list(BUCKET, Storage.BlobListOption.prefix("$PREFIX/model-line/")))
      .thenReturn(page)
  }

  companion object {
    private const val BUCKET = "fixture-bucket"
    private const val PREFIX = "vid-labeled-impressions/edp/edp7"
    private const val GENERATION = 123L
    private val DATE = LocalDate.of(2021, 3, 21)
  }
}
