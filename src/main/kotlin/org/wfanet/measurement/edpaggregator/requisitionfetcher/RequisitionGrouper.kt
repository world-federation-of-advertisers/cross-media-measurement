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
import com.google.type.Interval
import com.google.type.interval
import org.wfanet.measurement.api.v2alpha.EventGroup
import java.util.logging.Level
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.getEventGroupRequest
import org.wfanet.measurement.api.v2alpha.refuseRequisitionRequest
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.requisitionEntry
import org.wfanet.measurement.edpaggregator.v1alpha.groupedRequisitions

/**
 * An interface to group a list of requisitions.
 *
 * This class provides functionality to categorize a collection of [Requisition] objects into
 * groups, facilitating efficient execution.
 *
 * @param requisitionValidator: The [RequisitionValidator] to use to validate the requisition.
 * @param requisitionsClient The gRPC client used to interact with requisitions.
 * @param throttler used to throttle gRPC requests
 */
abstract class RequisitionGrouper(
  private val requisitionValidator: RequisitionsValidator,
  private val requisitionsClient: RequisitionsCoroutineStub,
  private val eventGroupsClient: EventGroupsCoroutineStub,
  private val throttler: Throttler,
) {

  /**
   * Groups a list of disparate [Requisition] objects for execution.
   *
   * This method takes in a list of [Requisition] objects, maps them to their respective groups, and
   * then combines these groups into a single list of [GroupedRequisitions].
   *
   * @param requisitions A list of [Requisition] objects to be grouped.
   * @return A list of [GroupedRequisitions] containing the categorized [Requisition] objects.
   */
  suspend fun groupRequisitions(requisitions: List<Requisition>): List<GroupedRequisitions> {
    val mappedRequisitions = requisitions.mapNotNull { mapRequisition(it) }
    return combineGroupedRequisitions(mappedRequisitions)
  }

  /** Function to be implemented to combine [GroupedRequisition]s for optimal execution. */
  protected suspend abstract fun combineGroupedRequisitions(
    groupedRequisitions: List<GroupedRequisitions>,
  ): List<GroupedRequisitions>

  /* Maps a single [Requisition] to a single [GroupedRequisition]. */
  private suspend fun mapRequisition(requisition: Requisition): GroupedRequisitions? {
    val measurementSpec: MeasurementSpec =
      try {
        requisitionValidator.validateMeasurementSpec(requisition)
      } catch (e: InvalidRequisitionException) {
        e.requisitions.forEach { refuseRequisition(it, e.refusal) }
        return null
      }
    return groupedRequisitions {
      modelLine = measurementSpec.modelLine
      this.requisitions += requisitionEntry { this.requisition = Any.pack(requisition) }
    }
  }

  protected suspend fun refuseRequisition(requisition: Requisition, refusal: Requisition.Refusal) {
    try {
      throttler.onReady {
        logger.info("Requisition ${requisition.name} was refused. $refusal")
        val request = refuseRequisitionRequest {
          this.name = requisition.name
          this.refusal = RequisitionKt.refusal { justification = refusal.justification }
        }
        requisitionsClient.refuseRequisition(request)
      }
    } catch (e: Exception) {
      logger.log(Level.SEVERE, "Error while refusing requisition ${requisition.name}", e)
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
