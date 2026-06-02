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

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.Truth.assertWithMessage
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerDatabaseAdmin
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

abstract class InProcessEdpAggregatorDirectMeasurementTest(
  kingdomDataServicesRule: ProviderRule<DataServices>,
  duchyDependenciesRule:
    ProviderRule<(String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies>,
  secureComputationDatabaseAdmin: SpannerDatabaseAdmin,
) :
  InProcessEdpAggregatorLifeOfAMeasurementIntegrationTest(
    kingdomDataServicesRule,
    duchyDependenciesRule,
    secureComputationDatabaseAdmin,
    hmssEnabled = false,
    trusTeeEnabled = false,
  ) {

  @Test
  fun `create a direct RF measurement and check the result is equal to the expected result`() =
    runBlocking {
      mcSimulator.testDirectReachAndFrequency(runId = "1234", numMeasurements = 1)
    }

  @Test
  fun `create a direct reach only measurement and check the result is equal to the expected result`() =
    runBlocking {
      mcSimulator.testDirectReachOnly(runId = "1234", numMeasurements = 1)
    }

  @Test
  fun `create incremental direct reach only measurements in same report and check the result is equal to the expected result`() =
    runBlocking {
      mcSimulator.testDirectReachOnly(runId = "1234", numMeasurements = 3)
    }

  @Test
  fun `create an impression measurement and check the result is equal to the expected result`() =
    runBlocking {
      mcSimulator.testImpression(
        "1234",
        eventGroupFilter = { it.eventGroupReferenceId !in MULTI_ENTITY_KEY_REF_IDS },
      )
    }

  @Test
  fun `direct measurement with multi-entity-key filtering returns correct subset`() = runBlocking {
    mcSimulator.testDirectReachAndFrequency(
      runId = "1235",
      numMeasurements = 1,
      eventGroupFilter = { it.eventGroupReferenceId in MULTI_ENTITY_KEY_REF_IDS },
    )
  }

  @Test
  fun `EDPA-registered EventGroups with entity_key round-trip to the CMMS public API`() =
    runBlocking {
      val edpDisplayNameToResourceMap = inProcessCmmsComponents.edpDisplayNameToResourceMap

      for ((edpDisplayName, refConfigs) in eventGroupConfigsByEdp) {
        val edpResourceName = edpDisplayNameToResourceMap.getValue(edpDisplayName).name
        val response =
          publicEventGroupsClient
            .withPrincipalName(edpResourceName)
            .listEventGroups(
              listEventGroupsRequest {
                parent = edpResourceName
                pageSize = 1000
                filter =
                  ListEventGroupsRequestKt.filter {
                    entityTypeIn += "campaign"
                    entityTypeIn += "ad_group"
                    entityTypeIn += "creative-id"
                  }
              }
            )

        val byRefId = response.eventGroupsList.associateBy { it.eventGroupReferenceId }
        val expectedRefIds =
          refConfigs.flatMap { (refId, config) ->
            when (config) {
              is EventGroupConfig.LegacySpec -> listOf(refId)
              is EventGroupConfig.MultiEntityKey ->
                config.entityKeySpecs.map { spec ->
                  "${spec.entityKey.entityType}/${spec.entityKey.entityId}"
                }
            }
          }
        assertWithMessage("EventGroups for $edpDisplayName")
          .that(byRefId.keys)
          .containsAtLeastElementsIn(expectedRefIds)

        for ((refId, config) in refConfigs) {
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
      val edpDisplayNameToResourceMap = inProcessCmmsComponents.edpDisplayNameToResourceMap
      val edpResourceName = edpDisplayNameToResourceMap.getValue("edp1").name

      val response =
        publicEventGroupsClient
          .withPrincipalName(edpResourceName)
          .listEventGroups(
            listEventGroupsRequest {
              parent = edpResourceName
              pageSize = 1000
            }
          )

      val legacy: EventGroup =
        response.eventGroupsList.single {
          it.eventGroupReferenceId == EDP_NO_ENTITY_KEY_EVENT_GROUP_REF_ID
        }
      assertThat(legacy.entityKey.entityType).isEqualTo("campaign")
      assertThat(legacy.entityKey.entityId).isEmpty()
      assertThat(legacy.eventGroupMetadata.hasEntityMetadata()).isFalse()
    }

  @Test
  fun `default ListEventGroups filter hides non-campaign EventGroups`() = runBlocking {
    val response =
      publicEventGroupsClient
        .withAuthenticationKey(mcApiKey)
        .listEventGroups(
          listEventGroupsRequest {
            parent = mcName
            pageSize = 1000
          }
        )

    val refIds = response.eventGroupsList.map { it.eventGroupReferenceId }.toSet()
    assertThat(refIds).contains(EDP_NO_ENTITY_KEY_EVENT_GROUP_REF_ID)
    for ((_, refConfigs) in eventGroupConfigsByEdp) {
      for ((_, config) in refConfigs) {
        if (config is EventGroupConfig.MultiEntityKey) {
          for (entityKeySpec in config.entityKeySpecs) {
            val derivedRefId =
              "${entityKeySpec.entityKey.entityType}/${entityKeySpec.entityKey.entityId}"
            assertThat(refIds).doesNotContain(derivedRefId)
          }
        }
      }
    }
  }
}
