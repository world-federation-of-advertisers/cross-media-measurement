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

package org.wfanet.measurement.edpaggregator.vidlabeler

import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.virtualpeople.common.LabelerInput
import org.wfanet.virtualpeople.common.LabelerOutput

@RunWith(JUnit4::class)
class VidModelLoaderTest {
  @Test
  fun `getAssigner loads once per model blob uri and reuses the cached assigner`() =
    runBlocking<Unit> {
      val loadCounts = mutableMapOf<String, Int>()
      val loader = VidModelLoader { uri ->
        loadCounts[uri] = (loadCounts[uri] ?: 0) + 1
        FakeVidAssigner()
      }

      val first = loader.getAssigner("model-a")
      val second = loader.getAssigner("model-a")
      val other = loader.getAssigner("model-b")

      assertThat(second).isSameInstanceAs(first)
      assertThat(other).isNotSameInstanceAs(first)
      assertThat(loadCounts).containsExactly("model-a", 1, "model-b", 1)
    }

  /** Distinct instance per construction so cache hits are observable via reference identity. */
  private class FakeVidAssigner : VidAssigner {
    override fun assign(input: LabelerInput): LabelerOutput = LabelerOutput.getDefaultInstance()
  }
}
