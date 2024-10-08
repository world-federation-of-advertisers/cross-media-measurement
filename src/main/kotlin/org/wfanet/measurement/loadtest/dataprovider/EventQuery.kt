// Copyright 2021 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.loadtest.dataprovider

import com.google.protobuf.Descriptors
import com.google.protobuf.Message
import org.projectnessie.cel.Program
import org.projectnessie.cel.common.types.BoolT
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpec.EventFilter
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters

/** A query to get the list of user virtual IDs for a particular requisition. */
interface EventQuery<out T : Message> {
  /**
   * An [EventGroup] with the specification of events from it.
   *
   * This is a [RequisitionSpec.EventGroupEntry] with the [EventGroup] dereferenced.
   */
  data class EventGroupSpec(
    val eventGroup: EventGroup,
    val spec: RequisitionSpec.EventGroupEntry.Value,
  )

  /** Returns a [Sequence] of [LabeledEvent]. */
  fun getLabeledEvents(eventGroupSpec: EventGroupSpec): Sequence<LabeledEvent<out T>>

  /**
   * Returns a [Sequence] of virtual person IDs for matching events.
   *
   * Each element in the returned value represents a single event. As a result, the same VID may be
   * returned multiple times.
   */
  fun getUserVirtualIds(eventGroupSpec: EventGroupSpec): Sequence<Long> {
    return getLabeledEvents(eventGroupSpec).map { it.vid }
  }

  /** Returns the virtual person ID universe represented by the [Sequence]. */
  fun getUserVirtualIdUniverse(): Sequence<Long>

  companion object {
    private val TRUE_EVAL_RESULT = Program.newEvalResult(BoolT.True, null)

    fun compileProgram(
      eventFilter: EventFilter,
      eventMessageDescriptor: Descriptors.Descriptor,
    ): Program {
      // EventFilters should take care of this, but checking here is an optimization that can skip
      // creation of a CEL Env.
      if (eventFilter.expression.isEmpty()) {
        return Program { TRUE_EVAL_RESULT }
      }
      return EventFilters.compileProgram(eventMessageDescriptor, eventFilter.expression)
    }
  }
}
