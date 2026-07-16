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
import com.google.protobuf.timestamp
import com.google.type.interval
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup as EdpaEventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.AdMetadataKt.campaignMetadata as edpaCampaignMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.adMetadata as edpaAdMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.entityKey as edpaEntityKey
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.metadata as edpaEventGroupMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroup as edpaEventGroup
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
  hmssEnabled: Boolean,
  trusTeeEnabled: Boolean,
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

  /**
   * Builds a valid EDPA-side source [EdpaEventGroup] for driving [EventGroupSync] directly in these
   * migration tests. Sets whichever of `event_group_reference_id` / `entity_key` is provided
   * (mirroring how an EDP evolves a row across the #4175 migration), plus the fields
   * `EventGroupSync.validateEventGroup` requires: media type, data-availability interval, and
   * metadata. `campaign` names the campaign metadata so a mutation is observable across syncs.
   */
  private fun buildMigrationSourceEventGroup(
    referenceId: String?,
    entityType: String?,
    entityId: String?,
    campaign: String,
  ): EdpaEventGroup = edpaEventGroup {
    if (!referenceId.isNullOrEmpty()) {
      eventGroupReferenceId = referenceId
    }
    measurementConsumer = measurementConsumerName()
    if (entityType != null && entityId != null) {
      entityKey = edpaEntityKey {
        this.entityType = entityType
        this.entityId = entityId
      }
    }
    eventGroupMetadata = edpaEventGroupMetadata {
      adMetadata = edpaAdMetadata {
        campaignMetadata = edpaCampaignMetadata {
          brand = "migration-brand"
          this.campaign = campaign
        }
      }
    }
    dataAvailabilityInterval = interval {
      startTime = timestamp { seconds = 200 }
      endTime = timestamp { seconds = 300 }
    }
    mediaTypes += EdpaEventGroup.MediaType.VIDEO
  }

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

  @Test
  fun `EDPA EventGroup migrating from refId-only to entity_key preserves identity across syncs`() =
    runBlocking {
      // Drives the #4175 migration for a single EventGroup through the REAL in-process Kingdom
      // (real request_id idempotency + real per-(DataProvider, MeasurementConsumer) uniqueness on
      // the EventGroupsByEntityKey index), not a mock. Uses a fresh, dedicated identifier ("mig-*")
      // that no other test touches. The sync is configured with entity_key_types covering the
      // migrating type ("creative-id") — the required production configuration for a fleet that
      // uses non-"campaign" entity types (see EventGroupSyncConfig.entity_key_types). The invariant
      // proven at every phase: the migrating EventGroup maps to exactly one Kingdom resource name
      // that never changes, so no duplicate / extra row is created.
      val edp = "edp1"
      val migRefId = "mig-eg-ref"
      val migEntityId = "mig-eg-entity"
      // Must include "campaign" too: the row is created refId-only, which the Kingdom defaults to
      // entity_type="campaign", so a campaign-blind fetch would miss it on the phase-2 re-sync.
      val entityKeyTypes = listOf("campaign", CREATIVE_ID_ENTITY_TYPE)

      val baselineCount = listCmmsEventGroups(edp, entityKeyTypes).size

      // Phase 1: refId-only — CREATE. One new Kingdom row.
      val phase1 =
        syncEventGroups(
          edp,
          listOf(
            buildMigrationSourceEventGroup(
              referenceId = migRefId,
              entityType = null,
              entityId = null,
              campaign = "c1",
            )
          ),
          entityKeyTypes,
        )
      assertThat(phase1).hasSize(1)
      val resourceName = phase1.single().eventGroupResource
      assertThat(resourceName).isNotEmpty()
      assertThat(phase1.single().eventGroupReferenceId).isEqualTo(migRefId)
      assertThat(listCmmsEventGroups(edp, entityKeyTypes)).hasSize(baselineCount + 1)

      // Phase 2: add entity_key alongside refId — UPDATE (matched by refId, backfills entity_key).
      // No new row.
      val phase2 =
        syncEventGroups(
          edp,
          listOf(
            buildMigrationSourceEventGroup(
              referenceId = migRefId,
              entityType = CREATIVE_ID_ENTITY_TYPE,
              entityId = migEntityId,
              campaign = "c1",
            )
          ),
          entityKeyTypes,
        )
      assertThat(phase2).hasSize(1)
      assertThat(phase2.single().eventGroupResource).isEqualTo(resourceName)
      assertThat(listCmmsEventGroups(edp, entityKeyTypes)).hasSize(baselineCount + 1)
      val afterPhase2 = listCmmsEventGroups(edp, entityKeyTypes).single { it.name == resourceName }
      assertThat(afterPhase2.entityKey.entityType).isEqualTo(CREATIVE_ID_ENTITY_TYPE)
      assertThat(afterPhase2.entityKey.entityId).isEqualTo(migEntityId)

      // Phase 3: drop refId entirely — entity_key only — UPDATE (matched by entity_key). No new
      // row. This is the phase that regresses to a duplicate CREATE (ALREADY_EXISTS on
      // EventGroupsByEntityKey) if the sync is misconfigured without entity_key_types.
      val phase3 =
        syncEventGroups(
          edp,
          listOf(
            buildMigrationSourceEventGroup(
              referenceId = null,
              entityType = CREATIVE_ID_ENTITY_TYPE,
              entityId = migEntityId,
              campaign = "c1-final",
            )
          ),
          entityKeyTypes,
        )
      assertThat(phase3).hasSize(1)
      assertThat(phase3.single().eventGroupResource).isEqualTo(resourceName)
      assertThat(listCmmsEventGroups(edp, entityKeyTypes)).hasSize(baselineCount + 1)

      // Phase 3 dropped refId and renamed the campaign. Verify both mutations actually landed on
      // the Kingdom row (the UPDATE applied), not just that identity was preserved.
      val afterPhase3 = listCmmsEventGroups(edp, entityKeyTypes).single { it.name == resourceName }
      assertThat(afterPhase3.eventGroupReferenceId).isEmpty()
      assertThat(afterPhase3.entityKey.entityId).isEqualTo(migEntityId)
      assertThat(afterPhase3.eventGroupMetadata.adMetadata.campaignMetadata.campaignName)
        .isEqualTo("c1-final")

      // The Reporting API (what a user sees) shows exactly one row for the migrated EventGroup, and
      // it is the same Kingdom resource — no extras leaked through the Reporting read path.
      val reportingRows = listReportingEventGroups().filter { it.cmmsEventGroup == resourceName }
      assertThat(reportingRows).hasSize(1)
      assertThat(reportingRows.single().entityKey.entityType).isEqualTo(CREATIVE_ID_ENTITY_TYPE)
      assertThat(reportingRows.single().entityKey.entityId).isEqualTo(migEntityId)
    }

  @Test
  fun `EDPA EventGroup fleet migrating at different paces keeps Kingdom row count static`() =
    runBlocking {
      // Fleet-wide partial migration through the REAL Kingdom. Three dedicated EventGroups start
      // refId-only and migrate to entity_key independently at different paces. The invariant across
      // every sync: the Kingdom row count grows by exactly the number of distinct EventGroups (3)
      // and never more — an EventGroup mid-migration is never duplicated, and EventGroups that
      // migrate later don't disturb ones that already migrated. Configured with entity_key_types so
      // migrated rows stay visible to subsequent syncs (the production-correct configuration).
      val edp = "edp1"
      val refIds = listOf("fleet-ref-A", "fleet-ref-B", "fleet-ref-C")
      val entityIds =
        mapOf(refIds[0] to "fleet-id-A", refIds[1] to "fleet-id-B", refIds[2] to "fleet-id-C")
      val entityKeyTypes = listOf("campaign", CREATIVE_ID_ENTITY_TYPE)

      val baselineCount = listCmmsEventGroups(edp, entityKeyTypes).size

      fun sources(entityKeyed: Set<String>) =
        refIds.map { refId ->
          if (refId in entityKeyed) {
            buildMigrationSourceEventGroup(
              referenceId = refId,
              entityType = CREATIVE_ID_ENTITY_TYPE,
              entityId = entityIds.getValue(refId),
              campaign = "campaign",
            )
          } else {
            buildMigrationSourceEventGroup(
              referenceId = refId,
              entityType = null,
              entityId = null,
              campaign = "campaign",
            )
          }
        }

      // Sync 1: all three refId-only — three CREATEs.
      val sync1 = syncEventGroups(edp, sources(entityKeyed = emptySet()), entityKeyTypes)
      assertThat(sync1).hasSize(3)
      assertThat(listCmmsEventGroups(edp, entityKeyTypes)).hasSize(baselineCount + 3)
      val migratedResourceNames = sync1.map { it.eventGroupResource }.toSet()
      assertThat(migratedResourceNames).hasSize(3)

      // Sync 2: only A migrates. B, C stay refId-only.
      syncEventGroups(edp, sources(entityKeyed = setOf(refIds[0])), entityKeyTypes)
      assertThat(listCmmsEventGroups(edp, entityKeyTypes)).hasSize(baselineCount + 3)
      assertThat(
          listCmmsEventGroups(edp, entityKeyTypes)
            .filter { it.name in migratedResourceNames }
            .count { it.entityKey.entityId.isNotEmpty() }
        )
        .isEqualTo(1)

      // Sync 3: A and B migrated. C still refId-only.
      syncEventGroups(edp, sources(entityKeyed = setOf(refIds[0], refIds[1])), entityKeyTypes)
      assertThat(listCmmsEventGroups(edp, entityKeyTypes)).hasSize(baselineCount + 3)
      assertThat(
          listCmmsEventGroups(edp, entityKeyTypes)
            .filter { it.name in migratedResourceNames }
            .count { it.entityKey.entityId.isNotEmpty() }
        )
        .isEqualTo(2)

      // Sync 4: all three migrated (dual-keyed).
      syncEventGroups(edp, sources(entityKeyed = refIds.toSet()), entityKeyTypes)
      assertThat(listCmmsEventGroups(edp, entityKeyTypes)).hasSize(baselineCount + 3)
      assertThat(
          listCmmsEventGroups(edp, entityKeyTypes)
            .filter { it.name in migratedResourceNames }
            .count { it.entityKey.entityId.isNotEmpty() }
        )
        .isEqualTo(3)

      // Sync 5: A and B drop refId (entity_key only); C keeps refId. Still matched by entity_key,
      // so no duplicate rows.
      syncEventGroups(
        edp,
        listOf(
          buildMigrationSourceEventGroup(
            null,
            CREATIVE_ID_ENTITY_TYPE,
            entityIds.getValue(refIds[0]),
            "campaign",
          ),
          buildMigrationSourceEventGroup(
            null,
            CREATIVE_ID_ENTITY_TYPE,
            entityIds.getValue(refIds[1]),
            "campaign",
          ),
          buildMigrationSourceEventGroup(
            refIds[2],
            CREATIVE_ID_ENTITY_TYPE,
            entityIds.getValue(refIds[2]),
            "campaign",
          ),
        ),
        entityKeyTypes,
      )
      assertThat(listCmmsEventGroups(edp, entityKeyTypes)).hasSize(baselineCount + 3)
      assertThat(listCmmsEventGroups(edp, entityKeyTypes).map { it.name }.toSet())
        .containsAtLeastElementsIn(migratedResourceNames)

      // Reporting API sees exactly the three migrated rows, one per resource name — no extras.
      val reportingMigrated =
        listReportingEventGroups().filter { it.cmmsEventGroup in migratedResourceNames }
      assertThat(reportingMigrated.map { it.cmmsEventGroup }.toSet())
        .isEqualTo(migratedResourceNames)
      assertThat(reportingMigrated).hasSize(3)
    }

  @Test
  fun `EDPA EventGroup migration without entity_key_types silently fails to re-sync the migrated row (blocked by unique index)`() =
    runBlocking {
      // Guards the migration footgun documented on EventGroupSyncConfig.entity_key_types: when the
      // sync is NOT configured with entity_key_types, listEventGroups defaults to entity_type=
      // "campaign" only, so once an EventGroup migrates to a non-"campaign" entity type the next
      // sync can no longer see it, treats it as new, and issues a CREATE that collides on the
      // unique EventGroupsByEntityKey index. This test pins that behavior so the requirement to set
      // entity_key_types is explicit and regressions are caught. It intentionally uses the default
      // (empty) entityKeyTypes.
      val edp = "edp1"
      val migRefId = "mig-nofilter-ref"
      val migEntityId = "mig-nofilter-entity"
      // The sync is deliberately left unconfigured (the footgun under test), but the raw
      // lister must still enumerate every type the row could carry to observe true Kingdom
      // state.
      val presentEntityTypes = listOf("campaign", CREATIVE_ID_ENTITY_TYPE)

      val baselineCount = listCmmsEventGroups(edp, presentEntityTypes).size

      // Phase 1: refId-only — CREATE. Visible under the default campaign filter.
      val phase1 =
        syncEventGroups(
          edp,
          listOf(
            buildMigrationSourceEventGroup(
              referenceId = migRefId,
              entityType = null,
              entityId = null,
              campaign = "c1",
            )
          ),
        )
      assertThat(phase1).hasSize(1)
      val resourceName = phase1.single().eventGroupResource
      assertThat(listCmmsEventGroups(edp, presentEntityTypes)).hasSize(baselineCount + 1)

      // Phase 2: migrate to entity_key. Matched by refId (still campaign-typed at fetch time), so
      // this UPDATE succeeds and backfills the creative-id entity_key. No new row yet.
      val phase2 =
        syncEventGroups(
          edp,
          listOf(
            buildMigrationSourceEventGroup(
              referenceId = migRefId,
              entityType = CREATIVE_ID_ENTITY_TYPE,
              entityId = migEntityId,
              campaign = "c1",
            )
          ),
        )
      assertThat(phase2).hasSize(1)
      assertThat(phase2.single().eventGroupResource).isEqualTo(resourceName)
      assertThat(listCmmsEventGroups(edp, presentEntityTypes)).hasSize(baselineCount + 1)

      // Phase 3: entity_key-only re-sync. The row is now creative-id-typed, invisible to the
      // campaign-only default fetch, so the sync tries to CREATE it again and the Kingdom rejects
      // the duplicate on EventGroupsByEntityKey. EventGroupSync records the per-item failure and
      // emits no mapping — the write does not go through, so still no duplicate row lands, but the
      // sync silently fails to progress the item. This is the misconfiguration signature.
      val phase3 =
        syncEventGroups(
          edp,
          listOf(
            buildMigrationSourceEventGroup(
              referenceId = null,
              entityType = CREATIVE_ID_ENTITY_TYPE,
              entityId = migEntityId,
              campaign = "c1-final",
            )
          ),
        )
      assertThat(phase3).isEmpty()
      // No duplicate landed (the unique index blocked it), but the item never synced: the mutation
      // (campaign renamed to "c1-final") did not apply.
      assertThat(listCmmsEventGroups(edp, presentEntityTypes)).hasSize(baselineCount + 1)
      // The stored row still shows the pre-failure campaign ("c1"), proving the phase-3 mutation
      // was not applied.
      val afterPhase3 =
        listCmmsEventGroups(edp, presentEntityTypes).single { it.name == resourceName }
      assertThat(afterPhase3.eventGroupMetadata.adMetadata.campaignMetadata.campaignName)
        .isEqualTo("c1")
    }

  @Test
  fun `EDPA EventGroup with non-campaign entity_type silently drops updates when entity_key_types is unset`() =
    runBlocking {
      // Documents the (undesirable) behavior of a MISCONFIGURED EventGroupSync — entity_key_types
      // left unset — for an EventGroup created directly with a non-"campaign" entity_type (a
      // publisher onboarding a new campaign/entity type, with no refId migration involved). This is
      // the counterpart to the migration footgun above: the correct configuration sets
      // entity_key_types so these rows stay visible to the sync (see the migration tests, which
      // do).
      //
      // Because the row is entity-keyed from creation, its CREATE request_id is derived from the
      // entity_key and is stable across syncs. The campaign-default fetch never sees the
      // creative-id
      // row, so every sync takes the CREATE path. The first CREATE lands the row; every later
      // CREATE
      // carries the same request_id, so the Kingdom idempotent-replays it and returns the ORIGINAL
      // resource. Two consequences of the misconfiguration, both proven below:
      //   1. No duplicate rows / no explosion and no ALREADY_EXISTS failure — idempotency absorbs
      //      the redundant CREATE (unlike the migrated-then-dropped-refId case, where the differing
      //      request_id hits the unique index instead).
      //   2. Because every sync replays the original CREATE instead of issuing an UPDATE, later
      //      changes to the source (here, a renamed campaign) are SILENTLY DROPPED — they never
      //      reach the Kingdom row and no error signals the misconfiguration.
      val edp = "edp1"
      val entityId = "born-creative-entity"
      val presentEntityTypes = listOf("campaign", CREATIVE_ID_ENTITY_TYPE)

      val baselineCount = listCmmsEventGroups(edp, presentEntityTypes).size

      fun source(campaign: String) =
        listOf(
          buildMigrationSourceEventGroup(
            referenceId = null,
            entityType = CREATIVE_ID_ENTITY_TYPE,
            entityId = entityId,
            campaign = campaign,
          )
        )

      // Sync 1: entity_key_types unset (the misconfiguration). The row is new — CREATE succeeds.
      val sync1 = syncEventGroups(edp, source("c1"))
      assertThat(sync1).hasSize(1)
      val resourceName = sync1.single().eventGroupResource
      assertThat(resourceName).isNotEmpty()
      assertThat(listCmmsEventGroups(edp, presentEntityTypes)).hasSize(baselineCount + 1)

      // Sync 2: identical source. The campaign-default fetch still can't see the creative-id row,
      // so
      // the sync re-issues a CREATE with the same entity-key request_id; the Kingdom idempotent-
      // replays and returns the same resource. No duplicate row, no failure.
      val sync2 = syncEventGroups(edp, source("c1"))
      assertThat(sync2).hasSize(1)
      assertThat(sync2.single().eventGroupResource).isEqualTo(resourceName)
      assertThat(listCmmsEventGroups(edp, presentEntityTypes)).hasSize(baselineCount + 1)

      // Sync 3: the source renames the campaign to "c2". Still invisible to the fetch, so the sync
      // replays the original CREATE instead of updating — the rename is silently dropped.
      val sync3 = syncEventGroups(edp, source("c2"))
      assertThat(sync3).hasSize(1)
      assertThat(sync3.single().eventGroupResource).isEqualTo(resourceName)
      assertThat(listCmmsEventGroups(edp, presentEntityTypes)).hasSize(baselineCount + 1)
      // The stored row still shows the original campaign ("c1"); the "c2" update never landed.
      val afterSync3 =
        listCmmsEventGroups(edp, presentEntityTypes).single { it.name == resourceName }
      assertThat(afterSync3.eventGroupMetadata.adMetadata.campaignMetadata.campaignName)
        .isEqualTo("c1")
    }
}
