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
import com.google.protobuf.ByteString
import java.nio.file.Paths
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.virtualpeople.common.BranchNodeKt
import org.wfanet.virtualpeople.common.CompiledNode
import org.wfanet.virtualpeople.common.PopulationNodeKt
import org.wfanet.virtualpeople.common.branchNode
import org.wfanet.virtualpeople.common.compiledNode
import org.wfanet.virtualpeople.common.eventId
import org.wfanet.virtualpeople.common.labelerInput
import org.wfanet.virtualpeople.common.rankedPopulationNode

@RunWith(JUnit4::class)
class VirtualPeoplePoolEmitLabelerTest {
  @Test
  fun `fromCompiledNodeBlob reads a Riegeli CompiledNode list`() {
    val modelBlob: ByteString =
      MODEL_BLOB_PATH.toFile().inputStream().use { ByteString.readFrom(it) }

    // Loads without throwing: the blob is a Riegeli record list, so the pre-fix single-node
    // `CompiledNode.parseFrom(modelBlob)` threw here; the list reader succeeds.
    val labeler = VirtualPeoplePoolEmitLabeler.fromCompiledNodeBlob(modelBlob)
    val offsets = labeler.emit(labelerInput { eventId = eventId { id = "any-event" } })

    // `single_id_model` is a single-VID model with no `RankedPopulationNode`, so POOL_IDENTITY
    // emits no pool offsets. The point of this test is that the Riegeli list is read and the
    // `Labeler` builds — the load path the pre-fix loader broke.
    assertThat(offsets).isEmpty()
  }

  @Test
  fun `rankedSizesByPoolOffset maps every ranked pool offset, walking embedded subtrees`() {
    // A flat RankedPopulationNode covering two pool offsets ...
    val flat = compiledNode {
      rankedPopulationNode = rankedPopulationNode {
        rankedSize = 100
        pools += PopulationNodeKt.virtualPersonPool { populationOffset = 5 }
        pools += PopulationNodeKt.virtualPersonPool { populationOffset = 6 }
      }
    }
    // ... and a BranchNode subtree whose child is a RankedPopulationNode (the walk must recurse
    // into embedded branches, not just look at top-level records).
    val subtree = compiledNode {
      branchNode = branchNode {
        branches +=
          BranchNodeKt.branch {
            node = compiledNode {
              rankedPopulationNode = rankedPopulationNode {
                rankedSize = 200
                pools += PopulationNodeKt.virtualPersonPool { populationOffset = 7 }
              }
            }
          }
      }
    }

    val map = VirtualPeoplePoolEmitLabeler.rankedSizesByPoolOffset(listOf(flat, subtree))

    assertThat(map).containsExactly(5L, 100, 6L, 100, 7L, 200)
  }

  @Test
  fun `rankedSizesByPoolOffset fails loudly when a pool offset has two ranked sizes`() {
    val a = compiledNode {
      rankedPopulationNode = rankedPopulationNode {
        rankedSize = 100
        pools += PopulationNodeKt.virtualPersonPool { populationOffset = 5 }
      }
    }
    val b = compiledNode {
      rankedPopulationNode = rankedPopulationNode {
        rankedSize = 200
        pools += PopulationNodeKt.virtualPersonPool { populationOffset = 5 }
      }
    }

    assertFailsWith<IllegalStateException> {
      VirtualPeoplePoolEmitLabeler.rankedSizesByPoolOffset(listOf<CompiledNode>(a, b))
    }
  }

  companion object {
    /**
     * Riegeli-encoded `CompiledNode` list, vendored from `virtual-people-core-serving` v0.3.1 into
     * `src/main/resources/testing/labeler`. Shared with `VirtualPeopleVidAssignerTest`; exercises
     * the Riegeli-list read path in [VirtualPeoplePoolEmitLabeler.fromCompiledNodeBlob].
     */
    private val MODEL_BLOB_PATH =
      getRuntimePath(
        Paths.get(
          "wfa_measurement_system",
          "src",
          "main",
          "resources",
          "testing",
          "labeler",
          "single_id_model_riegeli_list",
        )
      )!!
  }
}
