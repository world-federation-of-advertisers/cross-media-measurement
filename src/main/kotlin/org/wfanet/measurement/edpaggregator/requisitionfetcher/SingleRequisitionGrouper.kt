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

import com.google.protobuf.Any
import java.util.UUID
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupMapEntry
import org.wfanet.measurement.edpaggregator.v1alpha.groupedRequisitions

/**
 * Maps each input [Requisition] to its own [GroupedRequisitions] with no combining. Intended for
 * debugging and test scenarios.
 *
 * Skips a requisition whose spec cannot be decrypted or whose event groups cannot be resolved
 * consistently.
 */
class SingleRequisitionGrouper(
  requisitionValidator: RequisitionsValidator,
  requisitionsClient: RequisitionsCoroutineStub,
  eventGroupsClient: EventGroupsCoroutineStub,
  kingdomMutationThrottler: Throttler,
  kingdomEventGroupThrottler: Throttler,
) :
  RequisitionGrouper(
    requisitionValidator,
    requisitionsClient,
    eventGroupsClient,
    kingdomMutationThrottler,
    kingdomEventGroupThrottler,
  ) {

  /**
   * Backwards-compatible constructor that uses a single [throttler] for both Kingdom mutations and
   * event-group reads. Prefer the primary constructor for new code so the two RPC families can be
   * paced independently.
   */
  constructor(
    requisitionValidator: RequisitionsValidator,
    requisitionsClient: RequisitionsCoroutineStub,
    eventGroupsClient: EventGroupsCoroutineStub,
    throttler: Throttler,
  ) : this(
    requisitionValidator,
    requisitionsClient,
    eventGroupsClient,
    kingdomMutationThrottler = throttler,
    kingdomEventGroupThrottler = throttler,
  )

  /**
   * Returns one [GroupedRequisitions] per input [requisitions] entry, skipping any that cannot be
   * grouped (e.g. spec decryption failure, inconsistent event-group selectors). Group IDs are
   * generated randomly per requisition.
   */
  suspend fun groupRequisitions(requisitions: List<Requisition>): List<GroupedRequisitions> {
    val result = mutableListOf<GroupedRequisitions>()
    for (requisition in requisitions) {
      val grouped = groupSingle(requisition, UUID.randomUUID().toString()) ?: continue
      result.add(grouped)
    }
    return result
  }

  /**
   * Returns a single [GroupedRequisitions] for [requisition] under [groupId], or `null` if the
   * requisition's spec or event groups cannot be resolved.
   */
  suspend fun groupSingle(requisition: Requisition, groupId: String): GroupedRequisitions? {
    val measurementSpec: MeasurementSpec = requisition.measurementSpec.unpack()
    val spec =
      try {
        requisitionValidator.validateRequisitionSpec(requisition)
      } catch (exception: InvalidRequisitionException) {
        return null
      }
    val eventGroupMapEntries =
      try {
        getEventGroupMapEntries(spec)
      } catch (exception: InconsistentEventGroupSelectorsException) {
        return null
      }
    return groupedRequisitions {
      modelLine = measurementSpec.modelLine
      requisitions +=
        GroupedRequisitionsKt.requisitionEntry { this.requisition = Any.pack(requisition) }
      eventGroupMap +=
        eventGroupMapEntries.map { (eventGroupName, entryDetails) ->
          eventGroupMapEntry {
            this.eventGroup = eventGroupName
            details = entryDetails
          }
        }
      this.groupId = groupId
    }
  }
}
