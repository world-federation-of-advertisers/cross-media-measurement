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
import kotlinx.coroutines.flow.collect
import org.wfanet.measurement.api.v2alpha.EventGroupActivitiesGrpcKt.EventGroupActivitiesCoroutineStub
import org.wfanet.measurement.api.v2alpha.addEventGroupActivitiesRequest
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.MappedEventGroup
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.storage.StorageClient

/** OpenTelemetry instruments used when publishing EventGroup activity. */
class EventGroupActivityPublisherMetrics(meter: Meter) {
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
 * time between them in [marketTimeZone]. Including both endpoint dates intentionally favors
 * over-reporting activity over omitting a potentially active date. All metadata entity keys are
 * joined to EventGroup resource names in the Mesos RecordIO map exposed by
 * [eventGroupMapStorageClient]. Existing historical activity is never deleted.
 */
class CmmsEventGroupActivityPublisher(
  private val eventGroupActivitiesClient: EventGroupActivitiesCoroutineStub,
  private val eventGroupMapStorageClient: StorageClient,
  private val eventGroupMapBlobKey: String,
  private val dataProviderName: String,
  private val marketTimeZone: ZoneId,
  private val metrics: EventGroupActivityPublisherMetrics,
) {
  init {
    require(eventGroupMapBlobKey.isNotEmpty()) { "eventGroupMapBlobKey must not be empty" }
  }

  /** Publishes activity and returns the number of distinct EventGroup/date pairs written. */
  suspend fun publish(impressionMetadata: Collection<ImpressionMetadata>): Int {
    val activitiesByEntityKey = mutableMapOf<EntityKey, MutableSet<LocalDate>>()
    for (metadata in impressionMetadata) {
      val activityDates = metadata.activityDates()
      for (entityKey in metadata.entityKeysList) {
        activitiesByEntityKey
          .getOrPut(EntityKey(entityKey.entityType, entityKey.entityId)) { mutableSetOf() }
          .addAll(activityDates)
      }
    }
    if (activitiesByEntityKey.isEmpty()) {
      return 0
    }

    val eventGroupMap = loadEventGroupMap(activitiesByEntityKey.keys)
    val unmatchedEntityKeyCount =
      activitiesByEntityKey.keys.count {
        it.entityType in eventGroupMap.entityTypes && it !in eventGroupMap.eventGroupsByEntityKey
      }
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
      for (eventGroupName in eventGroupMap.eventGroupsByEntityKey[entityKey].orEmpty()) {
        datesByEventGroup.getOrPut(eventGroupName) { mutableSetOf() }.addAll(dates)
      }
    }

    val eventGroupsByDates = linkedMapOf<List<LocalDate>, MutableList<String>>()
    for ((eventGroupName, dates) in datesByEventGroup.toSortedMap()) {
      eventGroupsByDates.getOrPut(dates.sorted()) { mutableListOf() }.add(eventGroupName)
    }
    for ((dates, eventGroupNames) in eventGroupsByDates) {
      for (dateChunk in dates.chunked(MAX_ACTIVITY_PAIRS)) {
        val eventGroupBatchSize = MAX_ACTIVITY_PAIRS / dateChunk.size
        for (eventGroupBatch in eventGroupNames.chunked(eventGroupBatchSize)) {
          addActivities(eventGroupBatch, dateChunk)
        }
      }
    }
    return datesByEventGroup.values.sumOf { it.size }
  }

  private suspend fun loadEventGroupMap(requestedEntityKeys: Set<EntityKey>): LoadedEventGroupMap {
    val blob =
      checkNotNull(eventGroupMapStorageClient.getBlob(eventGroupMapBlobKey)) {
        "EventGroup map blob not found: $eventGroupMapBlobKey"
      }
    val eventGroupsByEntityKey = mutableMapOf<EntityKey, MutableSet<String>>()
    val entityTypes = mutableSetOf<String>()
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
      entityTypes += entityKey.entityType
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
    return LoadedEventGroupMap(eventGroupsByEntityKey, entityTypes)
  }

  private suspend fun addActivities(eventGroupNames: List<String>, dates: List<LocalDate>) {
    try {
      eventGroupActivitiesClient.addEventGroupActivities(
        addEventGroupActivitiesRequest {
          parent = dataProviderName
          eventGroups += eventGroupNames
          this.dates += dates.map { it.toProtoDate() }
        }
      )
    } catch (e: StatusException) {
      throw Exception(
        "Error publishing EventGroup activity for ${eventGroupNames.size} EventGroups",
        e,
      )
    }
  }

  private data class EntityKey(val entityType: String, val entityId: String)

  private data class LoadedEventGroupMap(
    val eventGroupsByEntityKey: Map<EntityKey, Set<String>>,
    val entityTypes: Set<String>,
  )

  private fun ImpressionMetadata.activityDates(): Set<LocalDate> {
    val start: Instant = interval.startTime.toInstant()
    val end: Instant = interval.endTime.toInstant()
    require(!end.isBefore(start)) { "ImpressionMetadata interval end_time precedes start_time" }

    val startDate = start.atZone(marketTimeZone).toLocalDate()
    val endDate = end.atZone(marketTimeZone).toLocalDate()
    return generateSequence(startDate) { date -> date.plusDays(1).takeIf { it <= endDate } }.toSet()
  }

  companion object {
    private val logger: Logger = Logger.getLogger(CmmsEventGroupActivityPublisher::class.java.name)
    private const val MAX_ACTIVITY_PAIRS = 1000
  }
}
