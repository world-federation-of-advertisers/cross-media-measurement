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

import org.measurement.integration.k8s.testing.CloudTestDataConfig
import org.measurement.integration.k8s.testing.CloudTestDataConfig.SyntheticEventGroup
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.loadtest.dataprovider.EntityKey

object CloudTestDataConfigs {

  /**
   * Converts a [CloudTestDataConfig] proto into the [EventGroupConfig] map keyed by
   * event_group_reference_id.
   *
   * Legacy event groups (no entity key specs) produce [EventGroupConfig.LegacySpec] entries.
   * Entity-key event groups produce [EventGroupConfig.MultiEntityKey] entries.
   *
   * @param config the proto config
   * @param specResolver resolves a data spec resource path to a [SyntheticEventGroupSpec]
   */
  fun toSyntheticEventGroupMap(
    config: CloudTestDataConfig,
    specResolver: (String) -> SyntheticEventGroupSpec,
  ): Map<String, EventGroupConfig> {
    return config.eventGroupsList.associate { eg ->
      eg.eventGroupReferenceId to toEventGroupConfig(eg, specResolver)
    }
  }

  /**
   * Resolves a [CloudTestDataConfig] into a flat map where each entity-key sub-spec becomes its own
   * entry keyed by "${entityType}-${entityId}".
   *
   * This is the format expected by [EdpAggregatorMeasurementConsumerSimulator].
   */
  fun toResolvedEventGroupMap(
    config: CloudTestDataConfig,
    specResolver: (String) -> SyntheticEventGroupSpec,
  ): Map<String, EventGroupConfig> {
    return config.eventGroupsList
      .flatMap { eg ->
        val eventGroupConfig = toEventGroupConfig(eg, specResolver)
        when (eventGroupConfig) {
          is EventGroupConfig.LegacySpec -> listOf(eg.eventGroupReferenceId to eventGroupConfig)
          is EventGroupConfig.MultiEntityKey ->
            eventGroupConfig.entityKeySpecs.map { entityKeySpec ->
              "${entityKeySpec.entityKey.entityType}-${entityKeySpec.entityKey.entityId}" to
                EventGroupConfig.MultiEntityKey(listOf(entityKeySpec))
            }
        }
      }
      .toMap()
  }

  /**
   * Builds GenerateSyntheticData CLI arguments from the config, grouped by EDP name.
   *
   * Each entity-key spec becomes its own event group invocation so that the ResultsFulfiller can
   * match blobs by the resolved ref ID (${entityType}-${entityId}).
   *
   * Returns a map of edpName to the list of per-event-group CLI flag segments.
   */
  fun toGenerateSyntheticDataArgs(config: CloudTestDataConfig): Map<String, List<String>> {
    return config.eventGroupsList
      .groupBy { it.edpName }
      .mapValues { (_, eventGroups) -> eventGroups.flatMap { eg -> toEventGroupArgs(eg) } }
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

  private fun toEventGroupArgs(eg: SyntheticEventGroup): List<String> {
    if (eg.entityKeySpecsList.isEmpty()) {
      return listOf(
        "--event-group-reference-id=${eg.eventGroupReferenceId}",
        "--data-spec-resource-path=${eg.dataSpecResourcePath}",
        "--entity-key-type=campaign",
        "--entity-key-id=${eg.eventGroupReferenceId}",
      )
    }
    return eg.entityKeySpecsList.flatMap { eks ->
      listOf(
        "--event-group-reference-id=${eks.entityType}-${eks.entityId}",
        "--data-spec-resource-path=${eks.dataSpecResourcePath}",
        "--entity-key-type=${eks.entityType}",
        "--entity-key-id=${eks.entityId}",
      )
    }
  }
}
