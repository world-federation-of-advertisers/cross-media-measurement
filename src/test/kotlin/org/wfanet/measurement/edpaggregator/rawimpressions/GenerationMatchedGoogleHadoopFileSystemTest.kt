/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.rawimpressions

import com.google.cloud.storage.BlobId
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageException
import com.google.common.truth.Truth.assertThat
import java.io.IOException
import kotlin.test.assertFailsWith
import org.apache.hadoop.fs.Path
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever

@RunWith(JUnit4::class)
class GenerationMatchedGoogleHadoopFileSystemTest {
  @Test
  fun `generation fragment identifies original object and expected generation`() {
    val path =
      checkNotNull(
        parseGenerationMatchedPath(
          Path(generationMatchedBlobUri("gs://bucket/folder/file.parquet", 123L))
        )
      )

    assertThat(path.cleanPath.toString()).isEqualTo("gs://bucket/folder/file.parquet")
    assertThat(path.blobId).isEqualTo(BlobId.of("bucket", "folder/file.parquet"))
    assertThat(path.generation).isEqualTo(123L)
  }

  @Test
  fun `generation mismatch gives EDP actionable recovery instruction`() {
    val storage = mock<Storage>()
    val path =
      GenerationMatchedPath(
        Path("gs://bucket/file.parquet"),
        BlobId.of("bucket", "file.parquet"),
        123L,
      )
    whenever(storage.get(any<BlobId>(), any<Storage.BlobGetOption>()))
      .thenThrow(StorageException(412, "conditionNotMet"))

    val exception = assertFailsWith<IOException> { getGenerationMatchedBlob(storage, path) }

    assertThat(exception)
      .hasMessageThat()
      .contains("no longer matches its registered generation 123")
    assertThat(exception).hasMessageThat().contains("write a new done blob")
  }
}
