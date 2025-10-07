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

import io.grpc.StatusException
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.getEventGroupRequest
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.groupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupMapEntry
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt
import com.google.type.Interval
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions.EventGroupDetails
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupDetails

/**
 * Naively does not combine a set of requisition. Generally not recommended for production use
 * cases.
 */
class SingleRequisitionGrouper(
  requisitionValidator: RequisitionsValidator,
  eventGroupsClient: EventGroupsCoroutineStub,
  throttler: Throttler,
  requisitionsClient: RequisitionsCoroutineStub,
) : RequisitionGrouper(requisitionValidator, requisitionsClient, eventGroupsClient, throttler) {

  override suspend fun combineGroupedRequisitions(
    groupedRequisitions: List<GroupedRequisitions>
  ): List<GroupedRequisitions> {
    return groupedRequisitions
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
