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

package org.wfanet.measurement.edpaggregator.rawimpressions

/**
 * 12-byte truncation of a SHA-256 digest of an event id, split into the natural `(Long, Int)` pair
 * used as keys by `Bytes12IntMap`.
 *
 * Named to distinguish it from the labeler's `acting_fingerprint` (a FarmHash used for model-tree
 * routing and unranked VID assignment): this digest serves a different purpose — shard partitioning
 * and rank-table keying.
 *
 * Held as a regular data class — allocation cost is small relative to the SHA-256 computation that
 * produced it. Promote to a zero-allocation surface (e.g. a ThreadLocal output buffer) only if
 * profiling shows it material.
 *
 * Use [high] directly for shard partitioning (`digest.high % totalShards`) — SHA-256's avalanche
 * makes the upper Long a uniform shard hash on its own. The [low] Int exists only to extend
 * bucket-key resolution past 64 bits inside `Bytes12IntMap`; it is not part of the shard math.
 *
 * @property high big-endian bytes 0..7 of the SHA-256 output.
 * @property low big-endian bytes 8..11 of the SHA-256 output.
 */
data class EventIdDigest(val high: Long, val low: Int)
