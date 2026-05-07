/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.dataprovider

import com.google.protobuf.Descriptors
import com.google.protobuf.DynamicMessage
import java.time.ZoneId
import org.projectnessie.cel.Program
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.common.toRange
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters

/**
 * [EventQuery] backed by [SyntheticDataGeneration] over a v2alpha [PopulationSpec].
 *
 * The event message type is supplied separately as [eventMessageDescriptor] (as opposed to being
 * declared on the population spec): v2alpha [PopulationSpec] is event-message-agnostic and carries
 * its template attributes as packed [com.google.protobuf.Any]s instead.
 */
abstract class SyntheticGeneratorEventQuery(
  val populationSpec: PopulationSpec,
  private val eventMessageDescriptor: Descriptors.Descriptor,
  val timeZone: ZoneId,
) : EventQuery<DynamicMessage> {

  /** Returns the synthetic data spec for [eventGroup]. */
  protected abstract fun getSyntheticDataSpec(eventGroup: EventGroup): SyntheticEventGroupSpec

  override fun getLabeledEvents(
    eventGroupSpec: EventQuery.EventGroupSpec
  ): Sequence<LabeledEvent<DynamicMessage>> {
    val timeRange = eventGroupSpec.spec.collectionInterval.toRange()
    val syntheticDataSpec: SyntheticEventGroupSpec = getSyntheticDataSpec(eventGroupSpec.eventGroup)
    val program: Program =
      EventQuery.compileProgram(eventGroupSpec.spec.filter, eventMessageDescriptor)
    return SyntheticDataGeneration.generateEvents(
        DynamicMessage.getDefaultInstance(eventMessageDescriptor),
        populationSpec,
        syntheticDataSpec,
        timeRange,
        timeZone,
      )
      .flatMap { it.labeledEvents }
      .filter { EventFilters.matches(it.message, program) }
  }

  /**
   * Returns the universe of VIDs across all sub-populations of [populationSpec].
   *
   * VIDs are emitted as the union of [PopulationSpec.SubPopulation.vidRangesList], iterating each
   * range from `startVid` through `endVidInclusive` inclusive. Ranges across sub-populations are
   * required by [PopulationSpec] to be disjoint.
   */
  override fun getUserVirtualIdUniverse(): Sequence<Long> = sequence {
    for (subpopulation in populationSpec.subpopulationsList) {
      for (range in subpopulation.vidRangesList) {
        for (vid in range.startVid..range.endVidInclusive) {
          yield(vid)
        }
      }
    }
  }
}
