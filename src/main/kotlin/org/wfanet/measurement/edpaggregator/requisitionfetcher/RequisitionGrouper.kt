/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.requisitionfetcher

import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.wfanet.measurement.api.v2alpha.EventGroup as CmmsEventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.getEventGroupRequest
import org.wfanet.measurement.api.v2alpha.refuseRequisitionRequest
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions.EventGroupDetails
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupDetails

/**
 * Base class for grouping [Requisition]s, providing shared helpers that
 * - resolve [CmmsEventGroup] metadata into [EventGroupDetails], and
 * - refuse a [Requisition] to the Kingdom.
 *
 * Implementations are responsible for the actual grouping policy. This class does not call the
 * Requisition Metadata Service or any other orchestration RPCs; orchestration belongs to
 * [RequisitionFetcher].
 *
 * @param requisitionValidator validates requisitions and their measurement/requisition specs.
 * @param requisitionsClient client used to refuse requisitions to the Kingdom.
 * @param eventGroupsClient client used to retrieve [CmmsEventGroup] metadata.
 * @param kingdomMutationThrottler throttles outbound mutations to the Kingdom (e.g.
 *   `refuseRequisition`).
 * @param kingdomEventGroupThrottler throttles read RPCs against the Kingdom's EventGroups service.
 */
abstract class RequisitionGrouper(
  protected val requisitionValidator: RequisitionsValidator,
  private val requisitionsClient: RequisitionsCoroutineStub,
  private val eventGroupsClient: EventGroupsCoroutineStub,
  private val kingdomMutationThrottler: Throttler,
  private val kingdomEventGroupThrottler: Throttler,
) {

  private val cmmsEventGroupValues: ConcurrentHashMap<String, CmmsEventGroup> = ConcurrentHashMap()
  private val cmmsEventGroupLocks: ConcurrentHashMap<String, Mutex> = ConcurrentHashMap()

  /**
   * Builds a map of event group details for the given [RequisitionSpec].
   *
   * Iterates over event group entries in the spec, fetches each [CmmsEventGroup] (memoized across
   * calls), and aggregates collection intervals per event group, sorted by start time. Validates
   * selector consistency before returning.
   */
  protected suspend fun getEventGroupMapEntries(
    requisitionSpec: RequisitionSpec
  ): Map<String, EventGroupDetails> {
    val eventGroupMap = mutableMapOf<String, EventGroupDetails>()
    for (eventGroupEntry in requisitionSpec.events.eventGroupsList) {
      val eventGroupName = eventGroupEntry.key
      if (eventGroupName in eventGroupMap) {
        eventGroupMap[eventGroupName] =
          eventGroupMap
            .getValue(eventGroupName)
            .toBuilder()
            .apply {
              val newCollectionIntervalList =
                this.collectionIntervalsList + eventGroupEntry.value.collectionInterval
              this.collectionIntervalsList.clear()
              this.collectionIntervalsList +=
                newCollectionIntervalList.sortedBy { it.startTime.toInstant() }
            }
            .build()
      } else {
        eventGroupMap[eventGroupName] = eventGroupDetails {
          val eventGroup = getEventGroup(eventGroupName)
          this.eventGroupReferenceId = eventGroup.eventGroupReferenceId
          this.collectionIntervals += eventGroupEntry.value.collectionInterval
          if (eventGroup.hasEntityKey()) {
            this.entityKey =
              GroupedRequisitionsKt.EventGroupDetailsKt.entityKey {
                entityType = eventGroup.entityKey.entityType
                entityId = eventGroup.entityKey.entityId
              }
          }
        }
      }
    }

    requisitionValidator.validateEventGroupSelectors(eventGroupMap)

    return eventGroupMap
  }

  /**
   * Returns the [CmmsEventGroup] for the given resource name, fetching it from the Kingdom on first
   * request and memoizing the result.
   *
   * Concurrent callers for the same `name` share a single in-flight fetch: a per-name [Mutex]
   * serializes the cache-fill so the gRPC call (and the throttle wait) happens at most once.
   */
  private suspend fun getEventGroup(name: String): CmmsEventGroup {
    cmmsEventGroupValues[name]?.let {
      return it
    }
    val mutex = cmmsEventGroupLocks.computeIfAbsent(name) { Mutex() }
    mutex.withLock {
      cmmsEventGroupValues[name]?.let {
        return it
      }
      val fetched =
        kingdomEventGroupThrottler.onReady {
          eventGroupsClient.getEventGroup(getEventGroupRequest { this.name = name })
        }
      cmmsEventGroupValues[name] = fetched
      return fetched
    }
  }

  /**
   * Refuses a requisition to the Kingdom.
   *
   * ### High-Level Flow
   * 1. Logs the refusal locally.
   * 2. Sends a [refuseRequisitionRequest] via [RequisitionsCoroutineStub], paced by
   *    [kingdomMutationThrottler].
   * 3. Errors during refusal are caught and logged; this method does not surface refusal errors to
   *    the caller.
   *
   * @param requisition The requisition to refuse.
   * @param refusal The reason and message for the refusal.
   */
  suspend fun refuseRequisitionToCmms(requisition: Requisition, refusal: Requisition.Refusal) {
    try {
      kingdomMutationThrottler.onReady {
        logger.info("Requisition ${requisition.name} was refused. $refusal")
        val request = refuseRequisitionRequest {
          this.name = requisition.name
          this.refusal =
            RequisitionKt.refusal {
              justification = refusal.justification
              message = refusal.message
            }
        }
        requisitionsClient.refuseRequisition(request)
      }
    } catch (e: Exception) {
      logger.log(Level.SEVERE, "Error while refusing requisition ${requisition.name}", e)
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(RequisitionGrouper::class.java.name)
  }
}
