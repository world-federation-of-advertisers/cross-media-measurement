// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.common.compression

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.combinedEvents
import org.wfanet.panelmatch.common.compression.testing.AbstractCompressorTest
import org.wfanet.panelmatch.common.toByteString

@RunWith(JUnit4::class)
class NoOpCompressorTest : AbstractCompressorTest() {
  override val compressor = NoOpCompressor()
  private val eventList = listOf("a", "b", "c")
  override val events = combinedEvents { serializedEvents += eventList.map { it.toByteString() } }

  @Test
  fun `compressed data size is equal to uncompressed data size`() {
    val compressedData = compressor.compress(events.toByteString())
    assertThat(events.toByteString().size()).isEqualTo(compressedData.size())
  }
}
