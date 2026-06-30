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

import com.google.protobuf.ByteString
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
 * Thread-safe: the underlying [Labeler] holds only immutable post-construction state and mutates
 * only its per-call builder, so [emit] is safe to call concurrently from [SubpoolAssignmentSink]'s
 * batch workers.
 */
class VirtualPeoplePoolEmitLabeler(private val labeler: Labeler) : PoolEmitLabeler {
  override fun emit(input: LabelerInput): List<Long> =
    labeler.label(input, LabelingMode.POOL_IDENTITY).poolAssignmentsList.map { it.poolOffset }

  override fun close() {}

  companion object {
    /**
     * Builds a [VirtualPeoplePoolEmitLabeler] from the binary-serialized [CompiledNode] root of a
     * compiled VID model (the `model_blob_path` payload).
     */
    fun fromCompiledNodeBlob(modelBlob: ByteString): VirtualPeoplePoolEmitLabeler =
      VirtualPeoplePoolEmitLabeler(Labeler.build(CompiledNode.parseFrom(modelBlob)))
  }
}
