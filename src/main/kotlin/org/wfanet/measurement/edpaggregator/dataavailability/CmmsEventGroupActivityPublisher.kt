/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.dataavailability

import io.grpc.StatusException
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.Meter
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.util.logging.Logger
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.collect
import org.wfanet.measurement.api.v2alpha.EventGroupActivitiesGrpcKt.EventGroupActivitiesCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupActivityKey
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.batchUpdateEventGroupActivitiesRequest
import org.wfanet.measurement.api.v2alpha.eventGroupActivity
import org.wfanet.measurement.api.v2alpha.updateEventGroupActivityRequest
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.MappedEventGroup
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.storage.StorageClient

/** Publishes positive EventGroup activity derived from finalized impression metadata. */
fun interface EventGroupActivityPublisher {
  /** Publishes activity and returns the number of distinct EventGroup/date pairs written. */
  suspend fun publish(impressionMetadata: Collection<ImpressionMetadata>): Int
}

/** OpenTelemetry instruments used when publishing EventGroup activity. */
class EventGroupActivityPublisherMetrics(meter: Meter = Instrumentation.meter) {
  val mapRecordsMissingEntityKeyCounter: LongCounter =
    meter
      .counterBuilder("edpa.event_group_activity.map_records_missing_entity_key")
      .setDescription("Number of EventGroup map records skipped because entity_key is absent")
      .build()

  val unmappedEntityKeysCounter: LongCounter =
    meter
      .counterBuilder("edpa.event_group_activity.unmapped_entity_keys")
      .setDescription("Number of impression metadata entity keys absent from the EventGroup map")
      .build()

  companion object {
    internal val DATA_PROVIDER_NAME_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("data_provider_name")
  }
}

/**
 * Publishes EventGroup activity through the CMMS Public API.
 *
 * Each metadata interval is expanded to the local calendar dates containing either endpoint or any
 * time between them in [timeZone]. Including both endpoint dates intentionally favors
 * over-reporting activity over omitting a potentially active date. Metadata entity keys are joined
 * to EventGroup resource names in the Mesos RecordIO map exposed by [eventGroupMapStorageClient].
 * Existing historical activity is never deleted.
 */
class CmmsEventGroupActivityPublisher(
  private val eventGroupActivitiesClient: EventGroupActivitiesCoroutineStub,
  private val eventGroupMapStorageClient: StorageClient,
  private val eventGroupMapBlobKey: String,
  private val dataProviderName: String,
  private val entityKeyTypes: Set<String>,
  private val maxConcurrentRequests: Int,
  private val timeZone: ZoneId,
  private val metrics: EventGroupActivityPublisherMetrics = EventGroupActivityPublisherMetrics(),
) : EventGroupActivityPublisher {
  init {
    require(eventGroupMapBlobKey.isNotEmpty()) { "eventGroupMapBlobKey must not be empty" }
    require(entityKeyTypes.isNotEmpty()) { "entityKeyTypes must not be empty" }
    require(maxConcurrentRequests > 0) { "maxConcurrentRequests must be greater than zero" }
  }

  override suspend fun publish(impressionMetadata: Collection<ImpressionMetadata>): Int {
    val activitiesByEntityKey = mutableMapOf<EntityKey, MutableSet<LocalDate>>()
    for (metadata in impressionMetadata) {
      val activityDates = metadata.activityDates()
      for (entityKey in metadata.entityKeysList) {
        if (entityKey.entityType in entityKeyTypes) {
          activitiesByEntityKey
            .getOrPut(EntityKey(entityKey.entityType, entityKey.entityId)) { mutableSetOf() }
            .addAll(activityDates)
        }
      }
    }
    if (activitiesByEntityKey.isEmpty()) {
      return 0
    }

    val eventGroupsByEntityKey = loadEventGroupMap(activitiesByEntityKey.keys)
    val unmatchedEntityKeyCount = activitiesByEntityKey.keys.count { it !in eventGroupsByEntityKey }
    if (unmatchedEntityKeyCount > 0) {
      logger.severe(
        "Skipped $unmatchedEntityKeyCount impression metadata entity keys without EventGroup " +
          "mappings for $dataProviderName"
      )
      metrics.unmappedEntityKeysCounter.add(
        unmatchedEntityKeyCount.toLong(),
        Attributes.of(EventGroupActivityPublisherMetrics.DATA_PROVIDER_NAME_ATTR, dataProviderName),
      )
    }

    val datesByEventGroup = mutableMapOf<String, MutableSet<LocalDate>>()
    for ((entityKey, dates) in activitiesByEntityKey) {
      for (eventGroupName in eventGroupsByEntityKey[entityKey].orEmpty()) {
        datesByEventGroup.getOrPut(eventGroupName) { mutableSetOf() }.addAll(dates)
      }
    }

    val batches =
      datesByEventGroup.entries
        .sortedBy { it.key }
        .flatMap { (eventGroupName, dates) ->
          dates.sorted().chunked(MAX_BATCH_SIZE).map { datesChunk ->
            ActivityBatch(eventGroupName, datesChunk)
          }
        }
    for (requestWindow in batches.chunked(maxConcurrentRequests)) {
      coroutineScope { requestWindow.map { batch -> async { upsert(batch) } }.awaitAll() }
    }
    return datesByEventGroup.values.sumOf { it.size }
  }

  private suspend fun loadEventGroupMap(
    requestedEntityKeys: Set<EntityKey>
  ): Map<EntityKey, Set<String>> {
    val blob =
      checkNotNull(eventGroupMapStorageClient.getBlob(eventGroupMapBlobKey)) {
        "EventGroup map blob not found: $eventGroupMapBlobKey"
      }
    val eventGroupsByEntityKey = mutableMapOf<EntityKey, MutableSet<String>>()
    var missingEntityKeyCount = 0L
    blob.read().collect { serializedRecord ->
      val mappedEventGroup = MappedEventGroup.parseFrom(serializedRecord)
      if (
        !mappedEventGroup.hasEntityKey() ||
          mappedEventGroup.entityKey.entityType.isEmpty() ||
          mappedEventGroup.entityKey.entityId.isEmpty()
      ) {
        missingEntityKeyCount++
        return@collect
      }

      val entityKey =
        EntityKey(mappedEventGroup.entityKey.entityType, mappedEventGroup.entityKey.entityId)
      if (entityKey in requestedEntityKeys) {
        eventGroupsByEntityKey
          .getOrPut(entityKey) { mutableSetOf() }
          .add(mappedEventGroup.eventGroupResource)
      }
    }

    if (missingEntityKeyCount > 0) {
      logger.severe(
        "Skipped $missingEntityKeyCount EventGroup map records without entity keys for " +
          dataProviderName
      )
      metrics.mapRecordsMissingEntityKeyCounter.add(
        missingEntityKeyCount,
        Attributes.of(EventGroupActivityPublisherMetrics.DATA_PROVIDER_NAME_ATTR, dataProviderName),
      )
    }
    return eventGroupsByEntityKey
  }

  private suspend fun upsert(batch: ActivityBatch) {
    val eventGroupKey =
      requireNotNull(EventGroupKey.fromName(batch.eventGroupName)) {
        "Invalid EventGroup resource name: ${batch.eventGroupName}"
      }
    try {
      eventGroupActivitiesClient.batchUpdateEventGroupActivities(
        batchUpdateEventGroupActivitiesRequest {
          parent = batch.eventGroupName
          for (date in batch.dates) {
            requests += updateEventGroupActivityRequest {
              eventGroupActivity = eventGroupActivity {
                name =
                  EventGroupActivityKey(
                      eventGroupKey.dataProviderId,
                      eventGroupKey.eventGroupId,
                      date.toString(),
                    )
                    .toName()
                this.date = date.toProtoDate()
              }
              allowMissing = true
            }
          }
        }
      )
    } catch (e: StatusException) {
      throw Exception("Error publishing EventGroup activity for ${batch.eventGroupName}", e)
    }
  }

  private data class EntityKey(val entityType: String, val entityId: String)

  private data class ActivityBatch(val eventGroupName: String, val dates: List<LocalDate>)

  private fun ImpressionMetadata.activityDates(): Set<LocalDate> {
    val start: Instant = interval.startTime.toInstant()
    val end: Instant = interval.endTime.toInstant()
    require(!end.isBefore(start)) { "ImpressionMetadata interval end_time precedes start_time" }

    val startDate = start.atZone(timeZone).toLocalDate()
    val endDate = end.atZone(timeZone).toLocalDate()
    return generateSequence(startDate) { date -> date.plusDays(1).takeIf { it <= endDate } }.toSet()
  }

  companion object {
    private val logger: Logger = Logger.getLogger(CmmsEventGroupActivityPublisher::class.java.name)
    private const val MAX_BATCH_SIZE = 1000
  }
}
