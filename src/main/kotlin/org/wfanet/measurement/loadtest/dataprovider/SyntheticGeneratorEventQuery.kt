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
import com.google.protobuf.TypeRegistry
import java.time.ZoneId
import org.projectnessie.cel.Program
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.toRange
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters

/** [EventQuery] that uses [SyntheticDataGeneration]. */
abstract class SyntheticGeneratorEventQuery(
  val populationSpec: SyntheticPopulationSpec,
  private val eventMessageDescriptor: Descriptors.Descriptor,
  val timeZone: ZoneId,
) : EventQuery<DynamicMessage> {
  init {
    require(
      populationSpec.eventMessageTypeUrl == ProtoReflection.getTypeUrl(eventMessageDescriptor)
    ) {
      "Incorrect event message descriptor for population spec"
    }
  }

  constructor(
    populationSpec: SyntheticPopulationSpec,
    typeRegistry: TypeRegistry,
    timeZone: ZoneId,
  ) : this(
    populationSpec,
    typeRegistry.getDescriptorForTypeUrl(populationSpec.eventMessageTypeUrl),
    timeZone,
  )

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

  override fun getUserVirtualIdUniverse(): Sequence<Long> {
    // TODO(@kungfucraig): Use EDP support library to retrieve the universe.
    val vidRange = populationSpec.vidRange
    return (vidRange.start until vidRange.endExclusive).asSequence()
  }
}
