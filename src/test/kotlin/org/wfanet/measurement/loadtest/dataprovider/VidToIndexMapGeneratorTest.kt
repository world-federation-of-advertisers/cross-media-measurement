// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.dataprovider

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class VidToIndexMapGeneratorTest {
  @Test
  fun `empty vid universe causes the vid map generation to fail`() {
    val salt = ByteString.copyFromUtf8("salt")
    val vidUniverse: List<Long> = emptyList()
    val exception =
      assertFailsWith<IllegalArgumentException> {
        VidToIndexMapGenerator.generateMapping(salt, vidUniverse)
      }

    assertThat(exception).hasMessageThat().contains("universe")
  }

  @Test
  fun `the vid map is generated correctly`() {
    val salt = ByteString.copyFromUtf8("salt")
    val vidUniverse: List<Long> = (1L..10L).toList()
    val vidMap = VidToIndexMapGenerator.generateMapping(salt, vidUniverse)
    val sortedListOfIndexAndNormalizedHashValues = vidMap.values.toList()

    // Verifies that the normalized hashes are in non-decreasing order and the indices increase
    // incrementally.
    assert(
      sortedListOfIndexAndNormalizedHashValues
        .zipWithNext { a, b -> (a.second <= b.second) && (b.first == a.first + 1) }
        .all { it }
    )
  }
}
