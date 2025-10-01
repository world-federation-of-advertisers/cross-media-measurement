/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.protobuf.Message
import kotlinx.coroutines.flow.Flow

/**
 * Reads labeled events from a data source.
 *
 * @see StorageEventReader for the primary implementation using cloud storage
 * @see LabeledEvent for the event data structure
 */
interface EventReader<T : Message> {
  /**
   * Reads labeled events from the configured source and emits them as batched flows.
   *
   * This suspend function initiates the event reading process and returns a [Flow] that emits
   * batches of [LabeledEvent] instances.
   *
   * The returned flow is cold, meaning the reading operation starts when collection begins and can
   * be collected multiple times if needed.
   *
   * ## Error Handling
   *
   * Implementations must throw [ImpressionReadException] with appropriate error codes:
   * - [ImpressionReadException.Code.BLOB_NOT_FOUND] when the data source doesn't exist
   * - [ImpressionReadException.Code.INVALID_FORMAT] when data cannot be parsed
   *
   * @return a cold [Flow] that emits lists of [LabeledEvent] instances. Each list represents a
   *   batch of events, with batch size determined by the implementation's batching strategy.
   * @throws ImpressionReadException if the data source cannot be accessed, authentication fails, or
   *   the data format is invalid
   * @throws IllegalStateException if the reader is not properly configured
   */
  suspend fun readEvents(): Flow<List<LabeledEvent<T>>>
}
