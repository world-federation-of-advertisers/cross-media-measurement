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

import com.google.protobuf.DynamicMessage
import kotlinx.coroutines.flow.Flow

/**
 * Interface for event sources that provide streams of labeled events.
 * 
 * Event sources are responsible for reading events from various sources
 * (synthetic generators, storage, etc.) and providing them as a flow
 * for processing by the pipeline.
 */
interface EventSource {
  
  /**
   * Produces a flow of labeled events for processing.
   * 
   * @return A flow of LabeledEvent instances
   */
  suspend fun getEvents(): Flow<LabeledEvent<DynamicMessage>>
  
  /**
   * Gets the estimated total number of events this source will produce.
   * 
   * @return The estimated event count, or -1 if unknown
   */
  fun getEstimatedEventCount(): Long
  
  /**
   * Closes the event source and releases any resources.
   */
  suspend fun close()
}