/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.integration.k8s

import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.timestamp
import com.google.protobuf.util.JsonFormat
import com.google.type.interval
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.EventGroup as CmmsEventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub as KingdomEventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt as KingdomListEventGroupsRequestKt
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest as kingdomListEventGroupsRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withDefaultDeadline
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup as SourceEventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup.MediaType
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.AdMetadataKt.campaignMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.adMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.entityKey
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.metadata as eventGroupMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.MappedEventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroups as sourceEventGroups
import org.wfanet.measurement.reporting.v2alpha.EventGroup as ReportingEventGroup
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub as ReportingEventGroupsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsRequestKt as ReportingListEventGroupsRequestKt
import org.wfanet.measurement.reporting.v2alpha.listEventGroupsRequest as reportingListEventGroupsRequest
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient

/** Exercises EventGroupSync and both public read paths at realistic cardinality. */
class EventGroupSyncScaleTest {
  private val projectId = requiredEnvironmentVariable("GOOGLE_CLOUD_PROJECT")
  private val kingdomTarget = requiredEnvironmentVariable("KINGDOM_PUBLIC_API_TARGET")
  private val reportingTarget = requiredEnvironmentVariable("REPORTING_PUBLIC_API_TARGET")
  private val kingdomCertHost = requiredEnvironmentVariable("KINGDOM_PUBLIC_API_CERT_HOST")
  private val measurementConsumer = requiredEnvironmentVariable("MC_NAME")
  private val apiKey = requiredEnvironmentVariable("MC_API_KEY")
  private val bucket = requiredEnvironmentVariable("STORAGE_BUCKET")
  private val dataProvider = requiredEnvironmentVariable("EVENT_GROUP_SCALE_DATA_PROVIDER")
  private val seed = requiredEnvironmentVariable("EVENT_GROUP_SCALE_SEED")

  private val inputBlobKey = "$STORAGE_PREFIX/event-groups/event-groups.json"
  private val mapBlobKey = "$STORAGE_PREFIX/event-groups-map/event-groups.binpb"
  private val storage: Storage = StorageOptions.newBuilder().setProjectId(projectId).build().service
  private val mapStorageClient =
    MesosRecordIoStorageClient(
      SelectedStorageClient(
        SelectedStorageClient.parseBlobUri("gs://$bucket/$mapBlobKey"),
        rootDirectory = null,
        projectId = projectId,
      )
    )

  @Test
  fun `large JSON sync applies mixed mutations`() = runBlocking {
    val kingdomChannel =
      buildMutualTlsChannel(
        kingdomTarget,
        AbstractEdpAggregatorCorrectnessTest.MEASUREMENT_CONSUMER_SIGNING_CERTS,
        kingdomCertHost.ifEmpty { null },
      )
    val reportingChannel =
      buildMutualTlsChannel(reportingTarget, REPORTING_SIGNING_CERTS, hostName = null)
    try {
      val kingdomEventGroupsStub =
        KingdomEventGroupsCoroutineStub(kingdomChannel.withDefaultDeadline(RPC_DEADLINE))
          .withAuthenticationKey(apiKey)
      val reportingEventGroupsStub =
        ReportingEventGroupsCoroutineStub(reportingChannel.withDefaultDeadline(RPC_DEADLINE))
      val current = listKingdomEventGroups(kingdomEventGroupsStub).map(::toStableSourceEventGroup)
      val bootstrap = buildBootstrap(current)
      uploadAndAwaitMap(bootstrap.input, bootstrap.expectedMappedReferenceIds)
      val stable = listKingdomEventGroups(kingdomEventGroupsStub)
      assertThat(stable).hasSize(TARGET_EVENT_GROUP_COUNT)

      val stableSource = stable.map(::toStableSourceEventGroup)
      val selection =
        EventGroupSyncScaleTestData.selectMutations(
          currentReferenceIds = stableSource.map(SourceEventGroup::getEventGroupReferenceId),
          seed = seed,
          mutationCount = MUTATION_COUNT,
        )
      val mutation = buildMutation(stableSource, selection)
      uploadAndAwaitMap(mutation.input, mutation.expectedMappedReferenceIds)

      val after = listKingdomEventGroups(kingdomEventGroupsStub)
      assertThat(after).hasSize(TARGET_EVENT_GROUP_COUNT)
      val afterByReferenceId = after.associateBy(CmmsEventGroup::getEventGroupReferenceId)
      assertThat(afterByReferenceId.keys)
        .containsExactlyElementsIn(mutation.expectedActiveReferenceIds)
      assertThat(afterByReferenceId.keys).containsNoneIn(selection.deletedReferenceIds)
      assertThat(afterByReferenceId.keys).containsAtLeastElementsIn(selection.addedReferenceIds)
      val incorrectlyUpdatedReferenceIds =
        selection.updatedReferenceIds.filter { referenceId ->
          afterByReferenceId
            .getValue(referenceId)
            .eventGroupMetadata
            .adMetadata
            .campaignMetadata
            .campaignName != mutationCampaignName(seed)
        }
      assertThat(incorrectlyUpdatedReferenceIds).isEmpty()
      val stableByReferenceId = stable.associateBy(CmmsEventGroup::getEventGroupReferenceId)
      val changedOmittedReferenceIds =
        selection.omittedReferenceIds.filter { referenceId ->
          val after = afterByReferenceId.getValue(referenceId)
          val before = stableByReferenceId.getValue(referenceId)
          after.eventGroupMetadata != before.eventGroupMetadata ||
            after.dataAvailabilityInterval != before.dataAvailabilityInterval ||
            after.mediaTypesList != before.mediaTypesList ||
            after.entityKey != before.entityKey
        }
      assertThat(changedOmittedReferenceIds).isEmpty()

      val reportingEventGroups = listReportingEventGroups(reportingEventGroupsStub)
      assertThat(reportingEventGroups).hasSize(TARGET_EVENT_GROUP_COUNT)
      assertThat(reportingEventGroups.map(ReportingEventGroup::getEventGroupReferenceId))
        .containsExactlyElementsIn(mutation.expectedActiveReferenceIds)
      assertThat(reportingEventGroups.map(ReportingEventGroup::getCmmsDataProvider).toSet())
        .containsExactly(dataProvider)
    } finally {
      for (channel in listOf(kingdomChannel, reportingChannel)) {
        channel.shutdown()
        channel.awaitTermination(CHANNEL_SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
      }
    }
  }

  private data class SyncInput(
    val input: List<SourceEventGroup>,
    val expectedMappedReferenceIds: Set<String>,
    val expectedActiveReferenceIds: Set<String>,
  )

  private fun buildBootstrap(current: List<SourceEventGroup>): SyncInput {
    val sorted = current.sortedBy(SourceEventGroup::getEventGroupReferenceId)
    val retained = sorted.take(TARGET_EVENT_GROUP_COUNT)
    val extras = sorted.drop(TARGET_EVENT_GROUP_COUNT)
    val existingReferenceIds = sorted.map(SourceEventGroup::getEventGroupReferenceId)
    val newReferenceIds =
      EventGroupSyncScaleTestData.newReferenceIds(
        existingReferenceIds = existingReferenceIds,
        seed = seed,
        label = "bootstrap",
        count = TARGET_EVENT_GROUP_COUNT - retained.size,
      )
    val active = retained + newReferenceIds.map(::newSourceEventGroup)
    val activeReferenceIds = active.map(SourceEventGroup::getEventGroupReferenceId).toSet()
    return SyncInput(
      input = active + extras.map(::deletedSourceEventGroup),
      expectedMappedReferenceIds = activeReferenceIds,
      expectedActiveReferenceIds = activeReferenceIds,
    )
  }

  private fun buildMutation(
    current: List<SourceEventGroup>,
    selection: EventGroupSyncScaleTestData.MutationSelection,
  ): SyncInput {
    val currentByReferenceId =
      current.associateBy(SourceEventGroup::getEventGroupReferenceId).toMutableMap()
    val updated =
      selection.updatedReferenceIds.associateWith { referenceId ->
        sourceEventGroup(referenceId = referenceId, campaignName = mutationCampaignName(seed))
      }
    val added = selection.addedReferenceIds.associateWith(::newSourceEventGroup)

    val input =
      current
        .asSequence()
        .filterNot {
          it.eventGroupReferenceId in selection.deletedReferenceIds ||
            it.eventGroupReferenceId in selection.omittedReferenceIds
        }
        .map { updated[it.eventGroupReferenceId] ?: it }
        .toMutableList()
    input +=
      selection.deletedReferenceIds.map {
        deletedSourceEventGroup(currentByReferenceId.getValue(it))
      }
    input += added.values

    selection.deletedReferenceIds.forEach(currentByReferenceId::remove)
    currentByReferenceId.putAll(updated)
    currentByReferenceId.putAll(added)
    val expectedMapped =
      input
        .asSequence()
        .filter { it.state != SourceEventGroup.State.DELETED }
        .map(SourceEventGroup::getEventGroupReferenceId)
        .toSet()
    return SyncInput(
      input = input,
      expectedMappedReferenceIds = expectedMapped,
      expectedActiveReferenceIds = currentByReferenceId.keys.toSet(),
    )
  }

  private fun newSourceEventGroup(referenceId: String): SourceEventGroup =
    sourceEventGroup(referenceId = referenceId, campaignName = BASE_CAMPAIGN_NAME)

  private fun sourceEventGroup(referenceId: String, campaignName: String): SourceEventGroup {
    check(referenceId.startsWith(REFERENCE_ID_PREFIX)) { "Unexpected reference ID $referenceId" }
    val entityId = referenceId.removePrefix("campaign-")
    return eventGroup {
      eventGroupReferenceId = referenceId
      measurementConsumer = this@EventGroupSyncScaleTest.measurementConsumer
      dataAvailabilityInterval = interval {
        startTime = timestamp { seconds = DATA_AVAILABILITY_START.epochSecond }
        endTime = timestamp { seconds = DATA_AVAILABILITY_END.epochSecond }
      }
      this.eventGroupMetadata = eventGroupMetadata {
        this.adMetadata = adMetadata {
          this.campaignMetadata = campaignMetadata {
            brand = BRAND_NAME
            campaign = campaignName
          }
        }
      }
      entityKey = entityKey {
        entityType = ENTITY_TYPE
        this.entityId = entityId
      }
      mediaTypes += MediaType.VIDEO
    }
  }

  private fun deletedSourceEventGroup(existingEventGroup: SourceEventGroup): SourceEventGroup =
    eventGroup {
      eventGroupReferenceId = existingEventGroup.eventGroupReferenceId
      measurementConsumer = this@EventGroupSyncScaleTest.measurementConsumer
      entityKey = existingEventGroup.entityKey
      state = SourceEventGroup.State.DELETED
    }

  private fun toStableSourceEventGroup(eventGroup: CmmsEventGroup): SourceEventGroup {
    check(eventGroup.hasEntityKey()) { "Scale EventGroup ${eventGroup.name} has no entity key" }
    check(eventGroup.entityKey.entityType == ENTITY_TYPE) {
      "Scale EventGroup ${eventGroup.name} has entity type ${eventGroup.entityKey.entityType}"
    }
    check(eventGroup.eventGroupReferenceId.startsWith(REFERENCE_ID_PREFIX)) {
      "Scale EventGroup ${eventGroup.name} is not owned by this test"
    }
    return sourceEventGroup(
      referenceId = eventGroup.eventGroupReferenceId,
      campaignName = BASE_CAMPAIGN_NAME,
    )
  }

  private suspend fun listKingdomEventGroups(
    eventGroupsStub: KingdomEventGroupsCoroutineStub
  ): List<CmmsEventGroup> {
    val result = mutableListOf<CmmsEventGroup>()
    val seenPageTokens = mutableSetOf<String>()
    var pageToken = ""
    do {
      check(seenPageTokens.add(pageToken)) {
        "Kingdom ListEventGroups returned a repeated page token"
      }
      val response =
        eventGroupsStub.listEventGroups(
          kingdomListEventGroupsRequest {
            parent = measurementConsumer
            pageSize = PAGE_SIZE
            this.pageToken = pageToken
            filter =
              KingdomListEventGroupsRequestKt.filter {
                dataProviderIn += dataProvider
                entityTypeIn += ENTITY_TYPE
              }
          }
        )
      result += response.eventGroupsList
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return result
  }

  private suspend fun listReportingEventGroups(
    eventGroupsStub: ReportingEventGroupsCoroutineStub
  ): List<ReportingEventGroup> {
    val result = mutableListOf<ReportingEventGroup>()
    val seenPageTokens = mutableSetOf<String>()
    var pageToken = ""
    do {
      check(seenPageTokens.add(pageToken)) {
        "Reporting ListEventGroups returned a repeated page token"
      }
      val response =
        eventGroupsStub.listEventGroups(
          reportingListEventGroupsRequest {
            parent = measurementConsumer
            pageSize = PAGE_SIZE
            this.pageToken = pageToken
            structuredFilter =
              ReportingListEventGroupsRequestKt.filter {
                cmmsDataProviderIn += dataProvider
                entityTypeIn += ENTITY_TYPE
              }
          }
        )
      result += response.eventGroupsList
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return result
  }

  private suspend fun uploadAndAwaitMap(
    input: List<SourceEventGroup>,
    expectedMappedReferenceIds: Set<String>,
  ) {
    val previousGeneration = storage.get(bucket, mapBlobKey)?.generation
    val json = JsonFormat.printer().print(sourceEventGroups { eventGroups += input })
    storage.create(
      BlobInfo.newBuilder(BlobId.of(bucket, inputBlobKey))
        .setContentType("application/json")
        .build(),
      json.toByteArray(Charsets.UTF_8),
    )
    logger.info(
      "Uploaded ${input.size} EventGroups to gs://$bucket/$inputBlobKey; waiting for sync"
    )

    withTimeout(SYNC_TIMEOUT) {
      var lastObservedGeneration: Long? = null
      var lastObservedCount: Int? = null
      while (true) {
        val blob = storage.get(bucket, mapBlobKey)
        if (blob != null && blob.size > 0 && blob.generation != previousGeneration) {
          val mapped = readMappedEventGroups()
          val mappedReferenceIds =
            mapped.mapTo(mutableSetOf(), MappedEventGroup::getEventGroupReferenceId)
          if (
            mapped.size == expectedMappedReferenceIds.size &&
              mappedReferenceIds == expectedMappedReferenceIds
          ) {
            logger.info(
              "EventGroupSync produced ${mapped.size} mappings in generation ${blob.generation}"
            )
            break
          }
          if (blob.generation != lastObservedGeneration || mapped.size != lastObservedCount) {
            logger.info(
              "Observed map generation ${blob.generation} with ${mapped.size} mappings; " +
                "expected ${expectedMappedReferenceIds.size}"
            )
            lastObservedGeneration = blob.generation
            lastObservedCount = mapped.size
          }
        }
        delay(SYNC_POLL_INTERVAL)
      }
    }
  }

  private suspend fun readMappedEventGroups(): List<MappedEventGroup> {
    val blob = checkNotNull(mapStorageClient.getBlob(mapBlobKey))
    return blob.read().map(MappedEventGroup::parseFrom).toList()
  }

  private fun requiredEnvironmentVariable(name: String): String =
    checkNotNull(System.getenv(name)?.takeIf(String::isNotBlank)) { "$name must be set" }

  companion object {
    private val logger: Logger = Logger.getLogger(EventGroupSyncScaleTest::class.java.name)
    private val RPC_DEADLINE: Duration = Duration.ofSeconds(30)
    private val REPORTING_SIGNING_CERTS: SigningCerts by lazy {
      val secretFiles =
        AbstractEdpAggregatorCorrectnessTest.getRuntimePath(
          AbstractEdpAggregatorCorrectnessTest.SECRET_FILES_PATH
        )
      SigningCerts.fromPemFiles(
        secretFiles.resolve("mc_tls.pem").toFile(),
        secretFiles.resolve("mc_tls.key").toFile(),
        secretFiles.resolve("reporting_root.pem").toFile(),
      )
    }
    private val CHANNEL_SHUTDOWN_TIMEOUT: Duration = Duration.ofSeconds(5)
    private val SYNC_TIMEOUT = 20.minutes
    private val SYNC_POLL_INTERVAL = 5.seconds
    private val DATA_AVAILABILITY_START: Instant = Instant.parse("2025-01-01T00:00:00Z")
    private val DATA_AVAILABILITY_END: Instant = Instant.parse("2026-01-01T00:00:00Z")
    private const val STORAGE_PREFIX = "event_group_scale"
    private const val ENTITY_TYPE = "campaign"
    private const val REFERENCE_ID_PREFIX = "campaign-scale-test-"
    private const val BRAND_NAME = "event-group-scale-test"
    private const val BASE_CAMPAIGN_NAME = "stable"
    private const val TARGET_EVENT_GROUP_COUNT = 5_000
    private const val MUTATION_COUNT = 50
    private const val PAGE_SIZE = 500

    private fun mutationCampaignName(seed: String): String = "changed-$seed"
  }
}
