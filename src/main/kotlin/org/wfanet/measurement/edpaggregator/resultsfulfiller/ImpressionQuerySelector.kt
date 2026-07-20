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

import org.wfanet.measurement.edpaggregator.v1alpha.EntityKey

/**
 * Sealed class representing the two strategies for querying impression metadata.
 *
 * This replaces runtime if/else dispatch with a type-safe selector, following the same pattern as
 * [FilterSpec].
 */
sealed class ImpressionQuerySelector {
  /** Query impression metadata by entity key. */
  data class ByEntityKey(val entityKey: EntityKey) : ImpressionQuerySelector()

  /** Query impression metadata by event group reference ID. */
  data class ByEventGroupReferenceId(val refId: String) : ImpressionQuerySelector()

  /**
   * Query impression metadata by a set of event group reference IDs in a single request.
   *
   * Lets the caller batch many event groups that share a collection interval into one paginated
   * `ListImpressionMetadata` call instead of one call per event group.
   */
  data class ByEventGroupReferenceIds(val refIds: List<String>) : ImpressionQuerySelector()
}
