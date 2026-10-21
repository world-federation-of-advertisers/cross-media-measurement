// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.integration.common

import com.google.protobuf.Struct
import java.time.LocalDate
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.loadtest.dataprovider.EntityKey

/**
 * Maps an [EntityKey] to the [SyntheticEventGroupSpec] whose events should be tagged with that key
 * in the impressions blob.
 */
data class EntityKeySpec(
  val entityKey: EntityKey,
  val spec: SyntheticEventGroupSpec,
  val entityMetadata: Struct? = null,
)

/** Identifiers populated on an EventGroup configured by [EventGroupConfig.MultiEntityKey]. */
enum class EventGroupIdentifierForm {
  ENTITY_KEY_ONLY,
  REFERENCE_ID_AND_ENTITY_KEY,
}

/**
 * Test fixture for an EventGroup whose older impressions use a reference ID and whose newer
 * impressions use an entity key.
 *
 * @property entityKey entity key equivalent to the EventGroup config's reference ID.
 * @property entityKeyStartDate first date written with [entityKey]; earlier dates use the
 *   event-group reference ID.
 */
data class MixedIdentifierHistory(
  val entityKey: EntityKey,
  val entityKeyStartDate: LocalDate,
)

/**
 * Per-EventGroup configuration for the EDPA-side data generation, entity key registration, and blob
 * contents, keyed in [InProcessEdpAggregatorComponents] by `(edpAggregatorShortName,
 * eventGroupReferenceId)`.
 */
sealed class EventGroupConfig {
  abstract val mixedIdentifierHistory: MixedIdentifierHistory?

  data class LegacySpec(
    val spec: SyntheticEventGroupSpec,
    override val mixedIdentifierHistory: MixedIdentifierHistory? = null,
  ) : EventGroupConfig()

  data class MultiEntityKey(
    val entityKeySpecs: List<EntityKeySpec>,
    val eventGroupIdentifierForm: EventGroupIdentifierForm =
      EventGroupIdentifierForm.REFERENCE_ID_AND_ENTITY_KEY,
    override val mixedIdentifierHistory: MixedIdentifierHistory? = null,
  ) : EventGroupConfig() {
    init {
      if (mixedIdentifierHistory != null) {
        require(entityKeySpecs.single().entityKey == mixedIdentifierHistory.entityKey) {
          "Mixed identifier history must use the MultiEntityKey EventGroup's entity key"
        }
      }
    }
  }
}
