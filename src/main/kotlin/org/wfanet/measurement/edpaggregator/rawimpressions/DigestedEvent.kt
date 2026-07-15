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
 * One surviving raw impression event after shard-filtering, with its 12-byte [EventIdDigest]
 * pre-computed.
 *
 * [row] is the reader's representation of the decoded row; the type parameter [R] keeps this
 * reusable across readers. For the parquet reader ([RawImpressionSource]) `R` is `Map<String,
 * ParquetValue>` — keyed by **parquet column name** (NOT LabelerInput field name), each value a
 * typed `ParquetValue`; downstream consumers project it into whatever shape they need (e.g. a
 * `LabelerInput` proto) via the `labeler_input_field_mapping`.
 *
 * @property row the decoded row.
 * @property digest the event-id [EventIdDigest], or null when it was not computed (the non-memoized
 *   single-shard path skips the SHA-256); distinct from the labeler's `acting_fingerprint`.
 */
data class DigestedEvent<R>(val row: R, val digest: EventIdDigest?)
