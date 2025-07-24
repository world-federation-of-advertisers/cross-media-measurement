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

import com.google.protobuf.Any
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow

/**
 * Interface for event sources that provide events for processing.
 * 
 * This interface abstracts the source of events, allowing for different implementations
 * such as synthetic generation or reading from storage.
 */
interface EventSource {
  /**
   * Generates a flow of event batches.
   * 
   * @param dispatcher The coroutine context for parallel processing
   * @return A flow of event batches, where each batch is a list of labeled events
   */
  suspend fun generateEventBatches(
    dispatcher: CoroutineContext = Dispatchers.Default
  ): Flow<List<LabeledEvent<Any>>>
  
  /**
   * Generates a flow of individual events.
   * 
   * @return A flow of individual labeled events
   */
  suspend fun generateEvents(): Flow<LabeledEvent<Any>>
}