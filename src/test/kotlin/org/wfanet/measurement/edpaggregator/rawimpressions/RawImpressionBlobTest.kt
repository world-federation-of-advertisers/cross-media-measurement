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

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class RawImpressionBlobTest {
  @Test
  fun `generationMatchedBlobUri encodes GCS generation for Hadoop reader`() {
    val uri = generationMatchedBlobUri("gs://bucket/folder/file.parquet", 123L)

    assertThat(uri).isEqualTo("gs://bucket/.wfa-generation-match/123/folder/file.parquet")
    assertThat(java.net.URI.create(uri).authority).isEqualTo("bucket")
    assertThat(gcsHadoopConfiguration("project").get("fs.gs.impl"))
      .isEqualTo(GenerationMatchedGoogleHadoopFileSystem::class.java.name)
  }

  @Test
  fun `generationMatchedBlobUri rejects missing GCS generation`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        generationMatchedBlobUri("gs://bucket/file.parquet", 0L)
      }

    assertThat(exception).hasMessageThat().contains("missing its Cloud Storage generation")
  }

  @Test
  fun `generationMatchedBlobUri leaves local paths unchanged`() {
    assertThat(generationMatchedBlobUri("file:///tmp/file.parquet", 0L))
      .isEqualTo("file:///tmp/file.parquet")
    assertThat(generationMatchedBlobUri("relative/file.parquet", 0L))
      .isEqualTo("relative/file.parquet")
  }
}
