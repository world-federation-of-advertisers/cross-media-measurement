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
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupActivitiesGrpcKt.EventGroupActivitiesCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupActivityKey
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt.filter
import org.wfanet.measurement.api.v2alpha.batchUpdateEventGroupActivitiesRequest
import org.wfanet.measurement.api.v2alpha.eventGroupActivity
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.updateEventGroupActivityRequest
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata

/** Publishes positive EventGroup activity derived from finalized impression metadata. */
fun interface EventGroupActivityPublisher {
  /** Publishes activity and returns the number of distinct EventGroup/date pairs written. */
  suspend fun publish(impressionMetadata: Collection<ImpressionMetadata>): Int
}

/**
 * Publishes EventGroup activity through the CMMS Public API.
 *
 * Each metadata interval is expanded to the local calendar dates containing either endpoint or any
 * time between them in [timeZone]. Including both endpoint dates intentionally favors
 * over-reporting activity over omitting a potentially active date. Entity key types are matched
 * against EventGroups for [dataProviderName], then each distinct EventGroup/date pair is upserted
 * with `allow_missing`. Existing historical activity is never deleted.
 */
class CmmsEventGroupActivityPublisher(
  private val eventGroupsClient: EventGroupsCoroutineStub,
  private val eventGroupActivitiesClient: EventGroupActivitiesCoroutineStub,
  private val dataProviderName: String,
  private val entityKeyTypes: Set<String>,
  private val maxConcurrentRequests: Int,
  private val timeZone: ZoneId,
) : EventGroupActivityPublisher {
  init {
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

    val eventGroupsByEntityKey = listEventGroups().groupBy { it.toEntityKey() }
    val unmatchedEntityKeyCount = activitiesByEntityKey.keys.count { it !in eventGroupsByEntityKey }
    require(unmatchedEntityKeyCount == 0) {
      "$unmatchedEntityKeyCount configured entity keys did not match an EventGroup"
    }

    val datesByEventGroup = mutableMapOf<String, MutableSet<LocalDate>>()
    for ((entityKey, dates) in activitiesByEntityKey) {
      for (eventGroup in eventGroupsByEntityKey.getValue(entityKey)) {
        datesByEventGroup.getOrPut(eventGroup.name) { mutableSetOf() }.addAll(dates)
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

  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun listEventGroups(): List<EventGroup> {
    return eventGroupsClient
      .listResources { pageToken: String ->
        val response =
          try {
            eventGroupsClient.listEventGroups(
              listEventGroupsRequest {
                parent = dataProviderName
                pageSize = LIST_PAGE_SIZE
                this.pageToken = pageToken
                this.filter = filter { entityTypeIn += entityKeyTypes.sorted() }
              }
            )
          } catch (e: StatusException) {
            throw Exception("Error listing EventGroups", e)
          }
        ResourceList(response.eventGroupsList, response.nextPageToken)
      }
      .flattenConcat()
      .toList()
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

  private fun EventGroup.toEntityKey(): EntityKey =
    EntityKey(entityKey.entityType, entityKey.entityId)

  private fun ImpressionMetadata.activityDates(): Set<LocalDate> {
    val start: Instant = interval.startTime.toInstant()
    val end: Instant = interval.endTime.toInstant()
    require(!end.isBefore(start)) { "ImpressionMetadata interval end_time precedes start_time" }

    val startDate = start.atZone(timeZone).toLocalDate()
    val endDate = end.atZone(timeZone).toLocalDate()
    return generateSequence(startDate) { date -> date.plusDays(1).takeIf { it <= endDate } }.toSet()
  }

  companion object {
    private const val LIST_PAGE_SIZE = 500
    private const val MAX_BATCH_SIZE = 1000
  }
}
