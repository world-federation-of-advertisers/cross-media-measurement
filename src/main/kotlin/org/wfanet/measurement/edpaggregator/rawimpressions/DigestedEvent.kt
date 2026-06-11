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

import org.wfanet.measurement.storage.ParquetValue

/**
 * One surviving raw impression event after shard-filtering, with its 12-byte
 * [EventIdDigest] pre-computed.
 *
 * [row] is the full parquet row as returned by
 * [org.wfanet.measurement.storage.ParquetStorageClient.ParquetBlob.readRows]:
 * a `Map` keyed by **parquet column name** (NOT LabelerInput field name), with
 * each value a typed [ParquetValue] (a `oneof` over all parquet types; an absent
 * OPTIONAL column is a `ParquetValue` with `KIND_NOT_SET`). Consumers read a cell
 * by switching on [ParquetValue.kindCase] rather than casting out of `Any?`.
 *
 * Downstream consumers project [row] into whatever shape they need (typically a
 * `LabelerInput` proto or a market event-template) using the
 * `labeler_input_field_mapping` to translate semantic LabelerInput field paths to
 * the parquet column names that key this map.
 *
 * @property digest the event-id [EventIdDigest]; distinct from the labeler's
 *   `acting_fingerprint`.
 */
data class DigestedEvent(val row: Map<String, ParquetValue>, val digest: EventIdDigest)
