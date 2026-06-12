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

import com.google.protobuf.ByteString
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.wfanet.virtualpeople.common.CompiledNode
import org.wfanet.virtualpeople.common.LabelerInput
import org.wfanet.virtualpeople.common.LabelerOutput
import org.wfanet.virtualpeople.core.labeler.Labeler

/**
 * Assigns virtual person IDs (VIDs) to impressions by evaluating a compiled VirtualPeople model.
 *
 * This is a thin abstraction over the canonical [Labeler] from `virtual-people-core-serving`. The
 * VID-assignment math (model-tree traversal plus the hashing/Feistel logic) lives entirely in that
 * library — this interface exists only so the VID Labeler TEE app can depend on a small, testable
 * seam rather than the labeler implementation directly.
 */
interface VidAssigner {
  /**
   * Assigns VIDs to [input] using the bound model.
   *
   * @return the [LabelerOutput]; each assigned person is available via
   *   `output.getPeople(i).virtualPersonId`.
   */
  fun assign(input: LabelerInput): LabelerOutput
}

/**
 * [VidAssigner] backed by a VirtualPeople [Labeler] built from a single compiled model.
 *
 * One instance is bound to one model line's compiled model. Construct via [fromCompiledNodeBlob],
 * which parses a serialized [CompiledNode] (the `model_blob_path` payload resolved by the
 * `VidLabelingDispatcher`).
 */
class VirtualPeopleVidAssigner(private val labeler: Labeler) : VidAssigner {
  override fun assign(input: LabelerInput): LabelerOutput = labeler.label(input)

  companion object {
    /**
     * Builds a [VirtualPeopleVidAssigner] from the binary-serialized [CompiledNode] root of a
     * compiled VID model.
     */
    fun fromCompiledNodeBlob(modelBlob: ByteString): VirtualPeopleVidAssigner =
      VirtualPeopleVidAssigner(Labeler.build(CompiledNode.parseFrom(modelBlob)))
  }
}

/**
 * Loads and caches one [VidAssigner] per model blob URI so each compiled model is read from storage
 * and built into a [Labeler] at most once per process.
 *
 * @param loadAssigner builds a [VidAssigner] for a model blob URI — typically reads the serialized
 *   `CompiledNode` from storage and calls [VirtualPeopleVidAssigner.fromCompiledNodeBlob].
 *
 * TODO(world-federation-of-advertisers/cross-media-measurement#3899): replace with the shared EDP
 *   Aggregator VID model cache once it is available.
 */
class VidModelLoader(private val loadAssigner: suspend (modelBlobUri: String) -> VidAssigner) {
  private val mutex = Mutex()
  private val assignersByModelBlobUri = mutableMapOf<String, VidAssigner>()

  /** Returns the [VidAssigner] for [modelBlobUri], loading and caching it on first use. */
  suspend fun getAssigner(modelBlobUri: String): VidAssigner =
    mutex.withLock {
      assignersByModelBlobUri[modelBlobUri]
        ?: loadAssigner(modelBlobUri).also { assignersByModelBlobUri[modelBlobUri] = it }
    }
}
