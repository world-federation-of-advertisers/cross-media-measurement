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

import org.measurement.integration.k8s.testing.ImpressionTestDataConfig
import org.measurement.integration.k8s.testing.ImpressionTestDataConfig.SyntheticEventGroup
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.loadtest.dataprovider.EntityKey

object ImpressionTestDataConfigs {

  /**
   * Converts a [ImpressionTestDataConfig] proto into an [EventGroupConfig] map keyed by
   * event_group_reference_id. Multi-entity-key event groups are kept as a single
   * [EventGroupConfig.MultiEntityKey] entry with all their entity key specs.
   */
  fun toEventGroupMap(
    config: ImpressionTestDataConfig,
    specResolver: (String) -> SyntheticEventGroupSpec,
  ): Map<String, EventGroupConfig> {
    return config.eventGroupsList.associate { eg ->
      eg.eventGroupReferenceId to toEventGroupConfig(eg, specResolver)
    }
  }

  /**
   * Like [toEventGroupMap], but splits multi-entity-key event groups so each entity key spec
   * becomes its own entry keyed by "${entityType}-${entityId}".
   */
  fun toFlatEventGroupMap(
    config: ImpressionTestDataConfig,
    specResolver: (String) -> SyntheticEventGroupSpec,
  ): Map<String, EventGroupConfig> {
    return toEventGroupMap(config, specResolver)
      .flatMap { (key, config) ->
        when (config) {
          is EventGroupConfig.LegacySpec -> listOf(key to config)
          is EventGroupConfig.MultiEntityKey ->
            config.entityKeySpecs.map { entityKeySpec ->
              "${entityKeySpec.entityKey.entityType}-${entityKeySpec.entityKey.entityId}" to
                EventGroupConfig.MultiEntityKey(listOf(entityKeySpec))
            }
        }
      }
      .toMap()
  }

  private fun toEventGroupConfig(
    eg: SyntheticEventGroup,
    specResolver: (String) -> SyntheticEventGroupSpec,
  ): EventGroupConfig {
    return if (eg.entityKeySpecsList.isEmpty()) {
      EventGroupConfig.LegacySpec(specResolver(eg.dataSpecResourcePath))
    } else {
      EventGroupConfig.MultiEntityKey(
        eg.entityKeySpecsList.map { eksProto ->
          EntityKeySpec(
            entityKey = EntityKey(eksProto.entityType, eksProto.entityId),
            spec = specResolver(eksProto.dataSpecResourcePath),
            entityMetadata = if (eg.hasEntityMetadata()) eg.entityMetadata else null,
          )
        }
      )
    }
  }
}
