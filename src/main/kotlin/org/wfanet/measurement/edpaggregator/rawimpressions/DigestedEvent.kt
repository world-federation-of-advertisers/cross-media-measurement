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
 * A raw impression event that survived shard-filtering, tagged by whether its event-id
 * [EventIdDigest] was computed.
 *
 * [row] is the reader's representation of the decoded row; the type parameter [R] keeps this
 * reusable across readers. For the parquet reader ([RawImpressionSource]) `R` is `Map<String,
 * ParquetValue>` — keyed by **parquet column name** (NOT LabelerInput field name), each value a
 * typed `ParquetValue`; downstream consumers project it into whatever shape they need (e.g. a
 * `LabelerInput` proto) via the `labeler_input_field_mapping`.
 *
 * @property row the decoded row.
 */
sealed interface RawEvent<R> {
  val row: R
}

/**
 * A [RawEvent] whose event-id [EventIdDigest] was computed. Memoized paths (Phase-0 subpool
 * assignment and memoized Phase-2 labeling) require this variant, so the digest is non-null and the
 * compiler proves it is present at those call sites.
 *
 * @property digest the event-id [EventIdDigest]; distinct from the labeler's `acting_fingerprint`.
 */
data class DigestedEvent<R>(override val row: R, val digest: EventIdDigest) : RawEvent<R>

/**
 * A [RawEvent] emitted without a digest: the non-memoized single-shard Phase-2 path skips the
 * per-row SHA-256. Memoized consumers do not accept this variant, so routing one to a digest-keyed
 * consumer is a compile error rather than a runtime NPE.
 */
data class UndigestedEvent<R>(override val row: R) : RawEvent<R>
