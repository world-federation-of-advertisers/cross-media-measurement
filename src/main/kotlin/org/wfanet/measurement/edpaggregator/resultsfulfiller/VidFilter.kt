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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.common.hash.Hashing
import com.google.protobuf.Descriptors
import com.google.protobuf.DynamicMessage
import com.google.protobuf.TypeRegistry
import com.google.type.Interval
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import org.projectnessie.cel.Program
import org.projectnessie.cel.common.types.BoolT
import org.wfanet.measurement.api.v2alpha.RequisitionSpec.EventFilter
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters
import org.wfanet.sampling.VidSampler

object VidFilter {
  /**
   * Filters a flow of labeled impressions and extracts their VIDs.
   *
   * @param labeledImpressions The flow of labeled impressions to filter
   * @return A flow of VIDs from the filtered labeled impressions
   */
  fun filterAndExtractVids(
    labeledImpressions: Flow<LabeledImpression>,
    vidSamplingIntervalStart: Float,
    vidSamplingIntervalWidth: Float,
    eventFilter: EventFilter,
    collectionInterval: Interval,
    typeRegistry: TypeRegistry,
  ): Flow<Long> {
    return labeledImpressions
      .filter { labeledImpression ->
        isValidImpression(
          labeledImpression,
          vidSamplingIntervalStart,
          vidSamplingIntervalWidth,
          eventFilter,
          collectionInterval,
          typeRegistry,
        )
      }
      .map { labeledImpression -> labeledImpression.vid }
  }

  /**
   * Determines if an impression is valid based on various criteria.
   *
   * @param labeledImpression The impression to validate
   * @param vidSamplingIntervalStart The start of the VID sampling interval
   * @param vidSamplingIntervalWidth The width of the VID sampling interval
   * @param eventFilter The event filter criteria
   * @param collectionInterval The time interval for collection
   * @param typeRegistry The registry for looking up protobuf descriptors
   * @return True if the impression is valid, false otherwise
   */
  private fun isValidImpression(
    labeledImpression: LabeledImpression,
    vidSamplingIntervalStart: Float,
    vidSamplingIntervalWidth: Float,
    eventFilter: EventFilter,
    collectionInterval: Interval,
    typeRegistry: TypeRegistry,
  ): Boolean {
    require(
      vidSamplingIntervalStart < 1 &&
        vidSamplingIntervalStart >= 0 &&
        vidSamplingIntervalWidth > 0 &&
        vidSamplingIntervalStart + vidSamplingIntervalWidth <= 1.0
    ) {
      "Invalid vidSamplingInterval: start = $vidSamplingIntervalStart, width = " +
        "$vidSamplingIntervalWidth"
    }

    // Check if impression is within collection time interval
    val isInCollectionInterval =
      labeledImpression.eventTime.toInstant() >= collectionInterval.startTime.toInstant() &&
        labeledImpression.eventTime.toInstant() < collectionInterval.endTime.toInstant()

    if (!isInCollectionInterval) {
      return false
    }

    val sampler = VidSampler(Hashing.farmHashFingerprint64())
    // Check if VID is in sampling bucket
    val isInSamplingInterval =
      sampler.vidIsInSamplingBucket(
        labeledImpression.vid,
        vidSamplingIntervalStart,
        vidSamplingIntervalWidth,
      )

    if (!isInSamplingInterval) {
      return false
    }

    // Create filter program
    val eventMessageData = labeledImpression.event
    val eventTemplateDescriptor = typeRegistry.getDescriptorForTypeUrl(eventMessageData.typeUrl)
    val program = compileProgram(eventTemplateDescriptor, eventFilter.expression)
    val eventMessage = DynamicMessage.parseFrom(eventTemplateDescriptor, eventMessageData.value)

    // Pass event message through program
    val passesFilter = EventFilters.matches(eventMessage, program)

    return passesFilter
  }

  /**
   * Compiles a CEL program from an event filter and event message descriptor.
   *
   * @param eventFilter The event filter containing a CEL expression
   * @param eventMessageDescriptor The descriptor for the event message type
   * @return A compiled Program that can be used to filter events
   */
  private fun compileProgram(
    eventMessageDescriptor: Descriptors.Descriptor,
    filterExpression: String,
  ): Program {
    // EventFilters should take care of this, but checking here is an optimization that can skip
    // creation of a CEL Env.
    if (filterExpression.isEmpty()) {
      return Program { Program.newEvalResult(BoolT.True, null) }
    }

    return EventFilters.compileProgram(eventMessageDescriptor, filterExpression)
  }
}
