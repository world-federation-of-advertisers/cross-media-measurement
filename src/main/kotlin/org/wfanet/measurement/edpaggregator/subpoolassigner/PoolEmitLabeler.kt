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

import org.wfanet.virtualpeople.common.LabelerInput

/**
 * Runs the compiled VID model in *pool-emit* mode: given a projected [LabelerInput], resolves the
 * subpool offset(s) the event routes to.
 *
 * In pool-emit mode the model is evaluated only far enough to discover which `VirtualPersonPool`(s)
 * an event lands in (the `population_offset` of the pool reached by the model tree); no VID is
 * assigned. The Phase-0 SubpoolAssigner uses the result to bucket each fingerprint by subpool.
 *
 * Why a list and not a single offset: the model permits **multiple virtual people per single input
 * event** via Multiplicity nodes — `wfa_virtual_people.LabelerEvent.virtual_person_activities` is
 * `repeated` ("we allow multiple VIDs for single input event"), and `multiplicity_impl` clones the
 * event so each clone traverses the tree independently and can reach a different `PopulationNode`,
 * hence a different pool. So one event can emit several pool offsets. This is distinct from — and
 * additive to — the fact that across events the same fingerprint commonly routes to several
 * subpools. Both fan-outs are expected and deduped by [SubpoolFingerprintsAccumulator].
 *
 * The production implementation is [VirtualPeoplePoolEmitLabeler], which runs the canonical Kotlin
 * `Labeler` in pool-emit mode.
 *
 * ## Concurrency
 *
 * [emit] is called concurrently from many batch-processing coroutines (see
 * [SubpoolAssignmentSink]); implementations MUST be safe for concurrent invocation.
 * [VirtualPeoplePoolEmitLabeler] satisfies this because the canonical `Labeler` is immutable after
 * construction and mutates only its per-call event builder.
 *
 * TODO(@Marco-Premier): wire [VirtualPeoplePoolEmitLabeler] into
 *   `SubpoolAssignerAppRunner.loadPoolEmitLabeler`, loading the compiled model from
 *   `SubpoolAssignerParams.model_blob_path` and projecting `event_template_field_mapping` onto the
 *   model's event-template message.
 */
interface PoolEmitLabeler : AutoCloseable {
  /**
   * Invokes [onPoolOffset] once for each pool offset [input] routes to (one per virtual person the
   * model emits for the event), in model-emission order, and returns how many offsets were emitted
   * (0 when the event routes to no ranked pool — e.g. it falls back to hash-based VID assignment
   * and therefore has no subpool). Duplicate offsets are permitted and are passed through —
   * [SubpoolFingerprintsAccumulator] dedupes per subpool.
   *
   * The callback form lets the consumer hand each primitive offset straight to the accumulator with
   * no intermediate boxed `List<Long>` allocation on this per-event hot path.
   */
  fun emit(input: LabelerInput, onPoolOffset: (Long) -> Unit): Int

  /**
   * Returns the configured ranked sub-range size (`RankedPopulationNode.ranked_size`) of the
   * subpool at [poolOffset]. A static property of the compiled model, used by the Phase-0
   * last-shard-out to stamp each `VidRankBuilderParams.subpool_ranked_sizes` entry so the Phase-1
   * ranker can size its rank `BitSet` and cap allocation without loading the model itself.
   */
  fun rankedSize(poolOffset: Long): Int

  override fun close() {}
}
