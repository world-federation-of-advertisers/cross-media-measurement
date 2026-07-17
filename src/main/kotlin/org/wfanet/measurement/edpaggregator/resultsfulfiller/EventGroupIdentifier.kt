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

import org.wfanet.measurement.edpaggregator.v1alpha.EntityKeyGroup

/**
 * Sealed class identifying how an [EventBatch] is associated with its event group.
 *
 * A batch is produced from a single blob whose metadata was queried by exactly one strategy. This
 * type encodes that strategy so downstream processors (e.g. [FilterProcessor]) can dispatch without
 * carrying two mutually-exclusive nullable fields.
 */
sealed class EventGroupIdentifier {
  data class ByReferenceId(val refId: String) : EventGroupIdentifier()

  data class ByEntityKeys(val entityKeys: List<EntityKeyGroup>) : EventGroupIdentifier()
}
