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

package org.wfanet.measurement.edpaggregator.subpoolassigner

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.TextFormat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.virtualpeople.common.CompiledNode
import org.wfanet.virtualpeople.common.eventId
import org.wfanet.virtualpeople.common.labelerInput

@RunWith(JUnit4::class)
class VirtualPeoplePoolEmitLabelerTest {

  private fun build(protoText: String): VirtualPeoplePoolEmitLabeler {
    val root = CompiledNode.newBuilder()
    TextFormat.merge(protoText, root)
    return VirtualPeoplePoolEmitLabeler.fromCompiledNodeBlob(root.build().toByteString())
  }

  @Test
  fun `emit returns the pool offset of the ranked leaf`() {
    val labeler =
      build(
        """
        name: "Root"
        ranked_population_node {
          pools { population_offset: 100 total_population: 500 }
          random_seed: "seed"
          ranked_size: 200
          unranked_mode: DISJOINT
        }
        """
          .trimIndent()
      )

    val offsets =
      labeler.emit(
        labelerInput {
          eventId = eventId {
            publisher = "test"
            id = "event-1"
          }
        }
      )

    assertThat(offsets).containsExactly(100L)
  }
}
