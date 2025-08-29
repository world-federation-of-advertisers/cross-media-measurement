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

import java.time.LocalDate

/**
 * Resolves paths for event blobs and their associated metadata.
 *
 * This interface abstracts the generation of storage paths, allowing for
 * different implementations including the future Impression Metadata Service.
 */
interface EventPathResolver {
  /**
   * Represents paths for a single event blob and its metadata.
   */
  data class EventPaths(
    val metadataPath: String,
    val eventGroupReferenceId: String
  )

  /**
   * Resolves paths for events based on date and event group reference ID.
   *
   * @param date The date for the events
   * @param eventGroupReferenceId The event group reference ID
   * @return The resolved paths for the event blob and metadata
   */
  suspend fun resolvePaths(date: LocalDate, eventGroupReferenceId: String): EventPaths
}

/**
 * Default implementation using the current path convention.
 * This will be replaced by the Impression Metadata Service in the future.
 */
class DefaultEventPathResolver(
  private val impressionsMetadataBucketUri: String
) : EventPathResolver {

  override suspend fun resolvePaths(
    date: LocalDate,
    eventGroupReferenceId: String
  ): EventPathResolver.EventPaths {
    val metadataPath = "$impressionsMetadataBucketUri/ds/$date/event-group-reference-id/$eventGroupReferenceId/metadata"

    return EventPathResolver.EventPaths(
      metadataPath = metadataPath,
      eventGroupReferenceId = eventGroupReferenceId
    )
  }
}
