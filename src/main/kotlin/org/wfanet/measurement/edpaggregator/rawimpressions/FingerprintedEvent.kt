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
 * One surviving raw impression event after window-filter +
 * shard-filter, with its 12-byte [Fingerprint] pre-computed.
 *
 * [row] is the full parquet row as returned by
 * [org.wfanet.measurement.storage.ParquetStorageClient.ParquetBlob.readRows]:
 * a `Map` keyed by **parquet column name** (NOT LabelerInput field
 * name), with native-typed values (`Long`, `Int`, `Double`, `Float`,
 * `Boolean`, `String`, `com.google.protobuf.ByteString`, or `null` for
 * OPTIONAL columns absent in this row). Includes the event-time
 * column (epoch micros, as a `Long`) — callers who need an `Instant`
 * pull it from [row] and convert.
 *
 * Downstream consumers project [row] into whatever shape they need
 * (typically a `LabelerInput` proto) using the
 * `labeler_input_field_mapping` to translate semantic LabelerInput
 * field paths to the parquet column names that key this map.
 */
data class FingerprintedEvent(val row: Map<String, Any?>, val fingerprint: Fingerprint)
