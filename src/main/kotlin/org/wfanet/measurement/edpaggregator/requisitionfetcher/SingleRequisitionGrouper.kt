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
import java.util.logging.Logger
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
 * Naively does not combine a set of requisition. Generally not recommended for production use
 * cases.
 */
class SingleRequisitionGrouper(
  private val requisitionValidator: RequisitionsValidator,
  eventGroupsClient: EventGroupsCoroutineStub,
  throttler: Throttler,
  requisitionsClient: RequisitionsCoroutineStub,
) : RequisitionGrouper(requisitionValidator, requisitionsClient, eventGroupsClient, throttler) {

  override suspend fun createGroupedRequisitions(
    requisitions: List<Requisition>
  ): List<GroupedRequisitions> {
    val groupedRequisitions = mutableListOf<GroupedRequisitions>()
    requisitions.forEach { requisition ->
      val measurementSpec: MeasurementSpec = requisition.measurementSpec.unpack()
      val spec =
        try {
          requisitionValidator.validateRequisitionSpec(requisition)
        } catch (exception: Exception) {
          return@forEach
        }
      // Get Event Group Map Entries
      val eventGroupMapEntries =
        try {
          getEventGroupMapEntries(spec)
        } catch (exception: Exception) {
          return@forEach
        }
      groupedRequisitions.add(
        groupedRequisitions {
          modelLine = measurementSpec.modelLine
          this.requisitions +=
            GroupedRequisitionsKt.requisitionEntry { this.requisition = Any.pack(requisition) }
          this.eventGroupMap +=
            eventGroupMapEntries.map {
              eventGroupMapEntry {
                this.eventGroup = it.key
                details = it.value
              }
            }
        }
      )
    }
    return groupedRequisitions
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
