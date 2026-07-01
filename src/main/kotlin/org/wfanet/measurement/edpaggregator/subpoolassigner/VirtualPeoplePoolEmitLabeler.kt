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

import com.google.protobuf.Any as ProtoAny
import com.google.protobuf.ByteString
import org.wfanet.measurement.common.riegeli.Riegeli
import org.wfanet.virtualpeople.common.CompiledNode
import org.wfanet.virtualpeople.common.LabelerInput
import org.wfanet.virtualpeople.core.labeler.Labeler
import org.wfanet.virtualpeople.core.labeler.LabelingMode

/**
 * [PoolEmitLabeler] backed by the canonical VirtualPeople [Labeler] run in pool-emit
 * ([LabelingMode.POOL_IDENTITY]) mode.
 *
 * In `POOL_IDENTITY` mode the model is evaluated only far enough for each `RankedPopulationNode`
 * leaf the event reaches to emit a `PoolAssignment` (its `population_offset`) instead of assigning
 * a VID; [emit] returns those offsets. The same compiled [Labeler] (and the same `POOL_IDENTITY`
 * traversal) is used in Phase 2 with `FULL` mode, so Phase-0 pool assignment and Phase-2 labeling
 * agree by construction.
 *
 * [rankedSize] is answered from a `population_offset -> ranked_size` map built once from the
 * compiled model tree at construction (a static property of the model), so the Phase-0
 * last-shard-out can stamp `VidRankBuilderParams.subpool_ranked_sizes` without re-traversing.
 *
 * Thread-safe: the underlying [Labeler] holds only immutable post-construction state and mutates
 * only its per-call builder, and the ranked-size map is immutable, so [emit] and [rankedSize] are
 * safe to call concurrently from [SubpoolAssignmentSink]'s batch workers.
 */
class VirtualPeoplePoolEmitLabeler
private constructor(
  private val labeler: Labeler,
  private val rankedSizeByPoolOffset: Map<Long, Int>,
) : PoolEmitLabeler {
  override fun emit(input: LabelerInput): List<Long> =
    labeler.label(input, LabelingMode.POOL_IDENTITY).poolAssignmentsList.map { it.poolOffset }

  override fun rankedSize(poolOffset: Long): Int =
    requireNotNull(rankedSizeByPoolOffset[poolOffset]) {
      "No RankedPopulationNode in the compiled model covers pool_offset $poolOffset"
    }

  override fun close() {}

  companion object {
    /**
     * Builds a [VirtualPeoplePoolEmitLabeler] from a compiled VID model blob (the `model_blob_path`
     * payload).
     *
     * The blob is a Riegeli record file whose records are [ProtoAny]-packed [CompiledNode]s of the
     * model tree (the layout written by the VirtualPeople model compiler's `WriteRiegeliFile`), so
     * each record is read, unpacked from `Any`, and the model is built via the [Labeler.build] list
     * overload rather than parsed as a single root node. This mirrors
     * `VirtualPeopleVidAssigner.fromCompiledNodeBlob` so Phase-0 pool assignment and Phase-2
     * labeling load the identical model.
     */
    fun fromCompiledNodeBlob(modelBlob: ByteString): VirtualPeoplePoolEmitLabeler {
      val nodes: List<CompiledNode> =
        Riegeli.readRecords(modelBlob.newInput())
          .map { record -> ProtoAny.parseFrom(record).unpack(CompiledNode::class.java) }
          .toList()
      return VirtualPeoplePoolEmitLabeler(Labeler.build(nodes), rankedSizesByPoolOffset(nodes))
    }

    /**
     * Maps each ranked subpool's `population_offset` to its `RankedPopulationNode.ranked_size`. The
     * Riegeli list may hold flat nodes or subtrees with embedded `BranchNode` children, so seed the
     * walk with every record and recurse into any embedded children. Fails loudly if the same
     * offset appears with two different ranked sizes.
     */
    internal fun rankedSizesByPoolOffset(nodes: List<CompiledNode>): Map<Long, Int> {
      val rankedSizeByPoolOffset = mutableMapOf<Long, Int>()
      val stack = ArrayDeque<CompiledNode>().apply { nodes.forEach { addLast(it) } }
      while (stack.isNotEmpty()) {
        val node = stack.removeLast()
        when {
          node.hasRankedPopulationNode() -> {
            val rankedSize = node.rankedPopulationNode.rankedSize.toInt()
            for (pool in node.rankedPopulationNode.poolsList) {
              val existing = rankedSizeByPoolOffset.putIfAbsent(pool.populationOffset, rankedSize)
              check(existing == null || existing == rankedSize) {
                "Inconsistent ranked_size for pool_offset ${pool.populationOffset}: " +
                  "$existing vs $rankedSize"
              }
            }
          }
          node.hasBranchNode() ->
            node.branchNode.branchesList.forEach { if (it.hasNode()) stack.addLast(it.node) }
        }
      }
      return rankedSizeByPoolOffset
    }
  }
}
