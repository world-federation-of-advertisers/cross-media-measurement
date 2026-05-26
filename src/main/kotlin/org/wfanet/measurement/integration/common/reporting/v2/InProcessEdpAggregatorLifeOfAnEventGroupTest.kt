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

package org.wfanet.measurement.integration.common.reporting.v2

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.Truth.assertWithMessage
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerDatabaseAdmin
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.AccessServicesFactory
import org.wfanet.measurement.integration.common.EventGroupConfig
import org.wfanet.measurement.integration.common.InProcessDuchy
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.reporting.deploy.v2.common.service.Services
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

/**
 * Integration tests for EDPA EventGroup operations.
 *
 * This is abstract so that different implementations of dependencies can all run the same tests.
 */
abstract class InProcessEdpAggregatorLifeOfAnEventGroupTest(
  kingdomDataServicesRule: ProviderRule<DataServices>,
  duchyDependenciesRule:
    ProviderRule<(String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies>,
  secureComputationDatabaseAdmin: SpannerDatabaseAdmin,
  accessServicesFactory: AccessServicesFactory,
  reportingDataServicesProviderRule: ProviderRule<Services>,
  duchyNames: List<String> = ALL_DUCHY_NAMES,
  hmssEnabled: Boolean = true,
  trusTeeEnabled: Boolean = true,
) :
  InProcessEdpAggregatorLifeOfAReportTest(
    kingdomDataServicesRule,
    duchyDependenciesRule,
    secureComputationDatabaseAdmin,
    accessServicesFactory,
    reportingDataServicesProviderRule,
    duchyNames,
    hmssEnabled,
    trusTeeEnabled,
  ) {

  @Test
  fun `EDPA EventGroups with explicit entity_key round-trip to the Reporting API`() = runBlocking {
    val byRefId: Map<String, EventGroup> =
      listReportingEventGroups().associateBy { it.eventGroupReferenceId }

    for ((_, refConfigs) in eventGroupConfigsByEdp) {
      for ((_, config) in refConfigs) {
        when (config) {
          is EventGroupConfig.LegacySpec -> continue
          is EventGroupConfig.MultiEntityKey -> {
            for (entityKeySpec in config.entityKeySpecs) {
              val derivedRefId =
                "${entityKeySpec.entityKey.entityType}/${entityKeySpec.entityKey.entityId}"
              val eventGroup = byRefId.getValue(derivedRefId)
              assertWithMessage("entity_key.entity_type for $derivedRefId")
                .that(eventGroup.entityKey.entityType)
                .isEqualTo(entityKeySpec.entityKey.entityType)
              assertWithMessage("entity_key.entity_id for $derivedRefId")
                .that(eventGroup.entityKey.entityId)
                .isEqualTo(entityKeySpec.entityKey.entityId)
              assertWithMessage("entity_metadata for $derivedRefId")
                .that(eventGroup.eventGroupMetadata.entityMetadata)
                .isEqualTo(entityKeySpec.entityMetadata)
            }
          }
        }
      }
    }
  }

  @Test
  fun `EDPA EventGroups without entity_key default to campaign with no entity_id or metadata`() =
    runBlocking {
      val byRefId: Map<String, EventGroup> =
        listReportingEventGroups().associateBy { it.eventGroupReferenceId }

      val legacy: EventGroup = byRefId.getValue(EDP_NO_ENTITY_KEY_EVENT_GROUP_REF_ID)
      assertThat(legacy.entityKey.entityType).isEqualTo("campaign")
      assertThat(legacy.entityKey.entityId).isEmpty()
      assertThat(legacy.eventGroupMetadata.hasEntityMetadata()).isFalse()
    }

  @Test
  fun `EDPA EventGroups with non-default entity_type round-trip through Reporting`() = runBlocking {
    val byRefId: Map<String, EventGroup> =
      listReportingEventGroups().associateBy { it.eventGroupReferenceId }

    val adGroupEventGroup = byRefId.getValue("ad_group/$AD_GROUP_EDP_EVENT_GROUP_REF_ID")
    assertThat(adGroupEventGroup.entityKey.entityType).isEqualTo("ad_group")
    assertThat(adGroupEventGroup.entityKey.entityId).isEqualTo(AD_GROUP_EDP_EVENT_GROUP_REF_ID)
    assertThat(adGroupEventGroup.eventGroupMetadata.entityMetadata).isEqualTo(ENTITY_METADATA)
  }
}
