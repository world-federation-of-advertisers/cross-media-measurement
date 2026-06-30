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

package org.wfanet.measurement.edpaggregator.rawimpressions

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFile
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadFile

@RunWith(JUnit4::class)
class RawImpressionFileBinPackerTest {

  private fun file(resourceName: String, bytes: Long): RawImpressionUploadFile =
    rawImpressionUploadFile {
      name = resourceName
      sizeBytes = bytes
    }

  @Test
  fun `empty input produces no batches`() {
    assertThat(RawImpressionFileBinPacker.pack(emptyList(), maxFileBatchSizeBytes = 100)).isEmpty()
  }

  @Test
  fun `files within the cap pack into a single batch`() {
    val files = listOf(file("a", 10), file("b", 20), file("c", 30))

    val batches = RawImpressionFileBinPacker.pack(files, maxFileBatchSizeBytes = 100)

    assertThat(batches).hasSize(1)
    // Sorted largest-first within the batch (60 total <= 100).
    assertThat(batches[0]).containsExactly("c", "b", "a").inOrder()
  }

  @Test
  fun `a batch may be filled exactly up to the cap`() {
    val files = listOf(file("a", 60), file("b", 40))

    val batches = RawImpressionFileBinPacker.pack(files, maxFileBatchSizeBytes = 100)

    // 60 + 40 == 100 is allowed (inclusive bound), so both fit in one batch.
    assertThat(batches).hasSize(1)
    assertThat(batches[0]).containsExactly("a", "b").inOrder()
  }

  @Test
  fun `files over the cap split first-fit-decreasing across batches`() {
    // Sizes 60, 50, 40, 10 with cap 100. FFD largest-first:
    //   60 -> batch0[60]; 50 -> batch1[50]; 40 -> batch0[60,40]=100; 10 -> batch1[50,10]=60.
    val files = listOf(file("s10", 10), file("s40", 40), file("s50", 50), file("s60", 60))

    val batches = RawImpressionFileBinPacker.pack(files, maxFileBatchSizeBytes = 100)

    assertThat(batches).hasSize(2)
    assertThat(batches[0]).containsExactly("s60", "s40").inOrder()
    assertThat(batches[1]).containsExactly("s50", "s10").inOrder()
  }

  @Test
  fun `a single file at or above the cap gets its own batch`() {
    val files = listOf(file("big", 150), file("small", 30))

    val batches = RawImpressionFileBinPacker.pack(files, maxFileBatchSizeBytes = 100)

    assertThat(batches).hasSize(2)
    // The oversized file is isolated (best-effort); the rest pack normally.
    assertThat(batches[0]).containsExactly("big")
    assertThat(batches[1]).containsExactly("small")
  }

  @Test
  fun `equal-size files break ties by resource name for determinism`() {
    val files = listOf(file("c", 10), file("a", 10), file("b", 10))

    val batches = RawImpressionFileBinPacker.pack(files, maxFileBatchSizeBytes = 100)

    assertThat(batches).hasSize(1)
    assertThat(batches[0]).containsExactly("a", "b", "c").inOrder()
  }

  @Test
  fun `packing is independent of input order`() {
    val files = listOf(file("d", 40), file("c", 30), file("b", 20), file("a", 10))

    val fromOrder = RawImpressionFileBinPacker.pack(files, maxFileBatchSizeBytes = 50)
    val fromShuffled = RawImpressionFileBinPacker.pack(files.reversed(), maxFileBatchSizeBytes = 50)

    // Internal sort makes the result a pure function of the set + cap, not the input order.
    assertThat(fromOrder).isEqualTo(fromShuffled)
    assertThat(fromOrder).hasSize(2)
    assertThat(fromOrder[0]).containsExactly("d", "a").inOrder()
    assertThat(fromOrder[1]).containsExactly("c", "b").inOrder()
  }

  @Test
  fun `non-positive cap is rejected`() {
    assertFailsWith<IllegalArgumentException> {
      RawImpressionFileBinPacker.pack(listOf(file("a", 10)), maxFileBatchSizeBytes = 0)
    }
    assertFailsWith<IllegalArgumentException> {
      RawImpressionFileBinPacker.pack(listOf(file("a", 10)), maxFileBatchSizeBytes = -1)
    }
  }
}
