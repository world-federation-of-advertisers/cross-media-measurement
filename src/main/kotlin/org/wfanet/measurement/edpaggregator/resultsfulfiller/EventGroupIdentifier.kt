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
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression

/** Identifies how an [EventBatch]'s blob metadata represents its event group. */
sealed class EventGroupIdentifier {
  /** A legacy reference ID and its equivalent entity key, when configured. */
  data class ByReferenceId(val refId: String, val entityKey: LabeledImpression.EntityKey?) :
    EventGroupIdentifier()

  /** Entity keys and their equivalent reference ID when exactly one mapped entity is present. */
  data class ByEntityKeys(val entityKeys: List<EntityKeyGroup>, val refId: String?) :
    EventGroupIdentifier()
}
