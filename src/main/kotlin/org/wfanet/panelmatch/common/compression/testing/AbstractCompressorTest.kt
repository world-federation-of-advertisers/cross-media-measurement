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

package org.wfanet.panelmatch.common.compression.testing

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.wfanet.panelmatch.client.CombinedEvents
import org.wfanet.panelmatch.common.compression.Compressor
import org.wfanet.panelmatch.common.toByteString

abstract class AbstractCompressorTest {
  protected abstract val compressor: Compressor
  protected abstract val events: CombinedEvents

  @Test
  fun `compress data and then uncompress result should equal original data`() {
    val compressedData = compressor.compress(events.toByteString())
    val uncompressedData = compressor.uncompress(compressedData)
    assertThat(CombinedEvents.parseFrom(uncompressedData)).isEqualTo(events)
  }
}
