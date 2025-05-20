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

import com.google.common.hash.HashFunction
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
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.sampling.VidSampler
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters

/**
 * Filters labeled impressions based on various criteria.
 *
 * @param program The compiled CEL program used to filter events
 * @param collectionInterval The time interval for collection
 * @param vidSamplingIntervalStart The start of the VID sampling interval
 * @param vidSamplingIntervalWidth The width of the VID sampling interval
 * @param typeRegistry The registry for looking up protobuf descriptors
 */
class VidFilter(
  private val eventFilter: RequisitionSpec.EventFilter,
  private val collectionInterval: Interval,
  private val vidSamplingIntervalStart: Float,
  private val vidSamplingIntervalWidth: Float,
  private val typeRegistry: TypeRegistry
) {
  private val sampler = VidSampler(VID_SAMPLER_HASH_FUNCTION)

  /**
   * Filters a flow of labeled impressions and extracts their VIDs.
   *
   * @param labeledImpressions The flow of labeled impressions to filter
   * @return A flow of VIDs from the filtered labeled impressions
   */
  fun filterAndExtractVids(labeledImpressions: Flow<LabeledImpression>): Flow<Long> {
    return labeledImpressions
      .filter { labeledImpression -> isValidImpression(labeledImpression) }
      .map { labeledImpression -> labeledImpression.vid }
  }

  /**
   * Determines if an impression is valid based on various criteria.
   *
   * @param labeledImpression The impression to validate
   * @return True if the impression is valid, false otherwise
   */
  private fun isValidImpression(labeledImpression: LabeledImpression): Boolean {
    // Check if impression is within collection time interval
    val isInCollectionInterval =
      labeledImpression.eventTime.toInstant() >= collectionInterval.startTime.toInstant() &&
        labeledImpression.eventTime.toInstant() < collectionInterval.endTime.toInstant()
    
    if (!isInCollectionInterval) {
      return false
    }

    // Check if VID is in sampling bucket
    val isInSamplingInterval = sampler.vidIsInSamplingBucket(
      labeledImpression.vid,
      vidSamplingIntervalStart,
      vidSamplingIntervalWidth
    )
    
    if (!isInSamplingInterval) {
      return false
    }

    // Create filter program
    val eventMessageData = labeledImpression.event
    val eventTemplateDescriptor = typeRegistry.getDescriptorForTypeUrl(eventMessageData.typeUrl)
    val program = compileProgram(eventTemplateDescriptor)
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
  fun compileProgram(
    eventMessageDescriptor: Descriptors.Descriptor,
  ): Program {
    // EventFilters should take care of this, but checking here is an optimization that can skip
    // creation of a CEL Env.
    if (eventFilter.expression.isEmpty()) {
      return Program { TRUE_EVAL_RESULT }
    }
    return EventFilters.compileProgram(eventMessageDescriptor, eventFilter.expression)
  }

  companion object {
    private val VID_SAMPLER_HASH_FUNCTION: HashFunction = Hashing.farmHashFingerprint64()
    private val TRUE_EVAL_RESULT = Program.newEvalResult(BoolT.True, null)
  }
}
